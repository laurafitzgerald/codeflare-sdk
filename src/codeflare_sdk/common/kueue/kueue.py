# Copyright 2024 IBM, Red Hat
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Optional, List, Dict, Any
from codeflare_sdk.common import _kube_api_error_handling
from codeflare_sdk.common.kubernetes_cluster.auth import config_check, get_api_client
from kubernetes import client
from kubernetes.client.exceptions import ApiException

from ...common.utils import get_current_namespace


def get_default_kueue_name(namespace: str) -> Optional[str]:
    """
    Retrieves the default Kueue name from the provided namespace.

    This function attempts to fetch the local queues in the given namespace and checks if any of them is annotated
    as the default queue. If found, the name of the default queue is returned.

    The default queue is marked with the annotation "kueue.x-k8s.io/default-queue" set to "true."

    Args:
        namespace (str):
            The Kubernetes namespace where the local queues are located.

    Returns:
        Optional[str]:
            The name of the default queue if it exists, otherwise None.
    """
    try:
        config_check()
        api_instance = client.CustomObjectsApi(get_api_client())
        local_queues = api_instance.list_namespaced_custom_object(
            group="kueue.x-k8s.io",
            version="v1beta1",
            namespace=namespace,
            plural="localqueues",
        )
    except ApiException as e:  # pragma: no cover
        if e.status == 404 or e.status == 403:
            return
        else:
            return _kube_api_error_handling(e)
    for lq in local_queues["items"]:
        if (
            "annotations" in lq["metadata"]
            and "kueue.x-k8s.io/default-queue" in lq["metadata"]["annotations"]
            and lq["metadata"]["annotations"]["kueue.x-k8s.io/default-queue"].lower()
            == "true"
        ):
            return lq["metadata"]["name"]


def list_local_queues(
    namespace: Optional[str] = None, flavors: Optional[List[str]] = None
) -> List[dict]:
    """
    This function lists all local queues in the namespace provided.

    If no namespace is provided, it will use the current namespace. If flavors is provided, it will only return local
    queues that support all the flavors provided.

    Note:
        Depending on the version of the local queue API, the available flavors may not be present in the response.

    Args:
        namespace (str, optional):
            The namespace to list local queues from. Defaults to None.
        flavors (List[str], optional):
            The flavors to filter local queues by. Defaults to None.
    Returns:
        List[dict]:
            A list of dictionaries containing the name of the local queue and the available flavors
    """

    if namespace is None:  # pragma: no cover
        namespace = get_current_namespace()
    try:
        config_check()
        api_instance = client.CustomObjectsApi(get_api_client())
        local_queues = api_instance.list_namespaced_custom_object(
            group="kueue.x-k8s.io",
            version="v1beta1",
            namespace=namespace,
            plural="localqueues",
        )
    except ApiException as e:  # pragma: no cover
        return _kube_api_error_handling(e)
    to_return = []
    for lq in local_queues["items"]:
        item = {"name": lq["metadata"]["name"]}
        if "flavors" in lq["status"]:
            item["flavors"] = [f["name"] for f in lq["status"]["flavors"]]
            if flavors is not None and not set(flavors).issubset(set(item["flavors"])):
                continue
        elif flavors is not None:
            continue  # NOTE: may be indicative old local queue API and might be worth while raising or warning here
        to_return.append(item)
    return to_return


def local_queue_exists(namespace: str, local_queue_name: str) -> bool:
    """
    Checks if a local queue with the provided name exists in the given namespace.

    This function queries the local queues in the specified namespace and verifies if any queue matches the given name.

    Args:
        namespace (str):
            The namespace where the local queues are located.
        local_queue_name (str):
            The name of the local queue to check for existence.

    Returns:
        bool:
            True if the local queue exists, False otherwise.
    """
    try:
        config_check()
        api_instance = client.CustomObjectsApi(get_api_client())
        local_queues = api_instance.list_namespaced_custom_object(
            group="kueue.x-k8s.io",
            version="v1beta1",
            namespace=namespace,
            plural="localqueues",
        )
    except Exception as e:  # pragma: no cover
        return _kube_api_error_handling(e)
    # check if local queue with the name provided in cluster config exists
    for lq in local_queues["items"]:
        if lq["metadata"]["name"] == local_queue_name:
            return True
    return False


def add_queue_label(item: dict, namespace: str, local_queue: Optional[str]):
    """
    Adds a local queue name label to the provided item.

    If the local queue is not provided, the default local queue for the namespace is used. The function validates if the
    local queue exists, and if it does, the local queue name label is added to the resource metadata.

    Args:
        item (dict):
            The resource where the label will be added.
        namespace (str):
            The namespace of the local queue.
        local_queue (str, optional):
            The name of the local queue to use. Defaults to None.

    Raises:
        ValueError:
            If the provided or default local queue does not exist in the namespace.
    """
    lq_name = local_queue or get_default_kueue_name(namespace)
    if lq_name == None:
        return
    elif not local_queue_exists(namespace, lq_name):
        raise ValueError(
            "local_queue provided does not exist or is not in this namespace. Please provide the correct local_queue name in Cluster Configuration"
        )
    if not "labels" in item["metadata"]:
        item["metadata"]["labels"] = {}
    item["metadata"]["labels"].update({"kueue.x-k8s.io/queue-name": lq_name})


def get_queue_resource_info(
    namespace: Optional[str] = None, queue_name: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get detailed resource information for local queues including quotas and flavors.
    
    This function provides comprehensive resource information that users need to make
    informed decisions about cluster configuration, including:
    - Available resource quotas (CPU, memory, GPU, etc.)
    - Resource flavors and their specifications
    - Cluster queue information
    - Current utilization (when available)
    
    Args:
        namespace (str, optional):
            The namespace to query. Defaults to current namespace.
        queue_name (str, optional):
            Specific queue name to get info for. If None, returns info for all queues.
            
    Returns:
        Dict[str, Any]:
            Dictionary containing detailed resource information for each queue.
            Structure:
            {
                "queues": {
                    "queue_name": {
                        "cluster_queue": "cluster-queue-name",
                        "is_default": bool,
                        "flavors": [
                            {
                                "name": "flavor-name",
                                "resources": {
                                    "cpu": {"nominalQuota": "9", "borrowingLimit": "..."},
                                    "memory": {"nominalQuota": "36Gi", "borrowingLimit": "..."},
                                    "nvidia.com/gpu": {"nominalQuota": "1", "borrowingLimit": "..."}
                                },
                                "node_labels": {...},
                                "tolerations": [...]
                            }
                        ],
                        "status": {
                            "pending": int,
                            "admitted": int,
                            "running": int
                        }
                    }
                },
                "summary": {
                    "total_queues": int,
                    "default_queue": str,
                    "available_resources": {
                        "cpu": "total_cpu",
                        "memory": "total_memory",
                        "gpu": "total_gpu"
                    }
                }
            }
    """
    if namespace is None:
        namespace = get_current_namespace()
    
    try:
        config_check()
        api_instance = client.CustomObjectsApi(get_api_client())
        
        # Get local queues
        local_queues = api_instance.list_namespaced_custom_object(
            group="kueue.x-k8s.io",
            version="v1beta1",
            namespace=namespace,
            plural="localqueues",
        )
        
        # Get cluster queues
        cluster_queues = api_instance.list_cluster_custom_object(
            group="kueue.x-k8s.io",
            version="v1beta1",
            plural="clusterqueues",
        )
        
        # Get resource flavors
        resource_flavors = api_instance.list_cluster_custom_object(
            group="kueue.x-k8s.io",
            version="v1beta1",
            plural="resourceflavors",
        )
        
    except ApiException as e:
        return _kube_api_error_handling(e)
    
    # Build cluster queue lookup
    cluster_queue_lookup = {}
    for cq in cluster_queues["items"]:
        cluster_queue_lookup[cq["metadata"]["name"]] = cq
    
    # Build resource flavor lookup
    flavor_lookup = {}
    for rf in resource_flavors["items"]:
        flavor_lookup[rf["metadata"]["name"]] = rf
    
    result = {
        "queues": {},
        "summary": {
            "total_queues": 0,
            "default_queue": None,
            "available_resources": {
                "cpu": "0",
                "memory": "0Gi",
                "gpu": "0"
            }
        }
    }
    
    total_cpu = 0
    total_memory = 0
    total_gpu = 0
    
    for lq in local_queues["items"]:
        queue_name_val = lq["metadata"]["name"]
        
        # Filter by specific queue if requested
        if queue_name and queue_name_val != queue_name:
            continue
            
        # Check if this is the default queue
        is_default = (
            "annotations" in lq["metadata"]
            and "kueue.x-k8s.io/default-queue" in lq["metadata"]["annotations"]
            and lq["metadata"]["annotations"]["kueue.x-k8s.io/default-queue"].lower() == "true"
        )
        
        if is_default:
            result["summary"]["default_queue"] = queue_name_val
        
        cluster_queue_name = lq["spec"].get("clusterQueue", "")
        cluster_queue_info = cluster_queue_lookup.get(cluster_queue_name, {})
        
        queue_info = {
            "cluster_queue": cluster_queue_name,
            "is_default": is_default,
            "flavors": [],
            "status": {
                "pending": 0,
                "admitted": 0,
                "running": 0
            }
        }
        
        # Extract flavor information from cluster queue
        if "spec" in cluster_queue_info and "resourceGroups" in cluster_queue_info["spec"]:
            for rg in cluster_queue_info["spec"]["resourceGroups"]:
                for flavor in rg.get("flavors", []):
                    flavor_name = flavor["name"]
                    flavor_info = {
                        "name": flavor_name,
                        "resources": {},
                        "node_labels": {},
                        "tolerations": []
                    }
                    
                    # Get resource quotas
                    for resource in flavor.get("resources", []):
                        resource_name = resource["name"]
                        flavor_info["resources"][resource_name] = {
                            "nominalQuota": resource.get("nominalQuota", "0"),
                            "borrowingLimit": resource.get("borrowingLimit", "0")
                        }
                        
                        # Track totals for summary
                        if resource_name == "cpu":
                            try:
                                total_cpu += float(str(resource.get("nominalQuota", "0")).replace("m", "")) / 1000
                            except:
                                pass
                        elif resource_name == "memory":
                            try:
                                mem_str = str(resource.get("nominalQuota", "0Gi"))
                                if mem_str.endswith("Gi"):
                                    total_memory += float(mem_str[:-2])
                                elif mem_str.endswith("Mi"):
                                    total_memory += float(mem_str[:-2]) / 1024
                            except:
                                pass
                        elif "gpu" in resource_name.lower():
                            try:
                                total_gpu += float(resource.get("nominalQuota", "0"))
                            except:
                                pass
                    
                    # Get flavor details from resource flavor
                    if flavor_name in flavor_lookup:
                        rf_info = flavor_lookup[flavor_name]
                        flavor_info["node_labels"] = rf_info.get("spec", {}).get("nodeLabels", {})
                        flavor_info["tolerations"] = rf_info.get("spec", {}).get("tolerations", [])
                    
                    queue_info["flavors"].append(flavor_info)
        
        # Get queue status if available
        if "status" in lq:
            status = lq["status"]
            queue_info["status"]["pending"] = status.get("pendingWorkloads", 0)
            queue_info["status"]["admitted"] = status.get("admittedWorkloads", 0)
            queue_info["status"]["running"] = status.get("runningWorkloads", 0)
        
        result["queues"][queue_name_val] = queue_info
    
    # Update summary
    result["summary"]["total_queues"] = len(result["queues"])
    result["summary"]["available_resources"]["cpu"] = f"{total_cpu:.1f}"
    result["summary"]["available_resources"]["memory"] = f"{total_memory:.1f}Gi"
    result["summary"]["available_resources"]["gpu"] = f"{total_gpu:.0f}"
    
    return result


def get_resource_flavor_details(flavor_name: str) -> Dict[str, Any]:
    """
    Get detailed information about a specific resource flavor.
    
    Args:
        flavor_name (str):
            The name of the resource flavor to get details for.
            
    Returns:
        Dict[str, Any]:
            Dictionary containing detailed flavor information including:
            - Node labels and selectors
            - Tolerations
            - Resource specifications
            - Usage statistics (if available)
    """
    try:
        config_check()
        api_instance = client.CustomObjectsApi(get_api_client())
        
        flavor_info = api_instance.get_cluster_custom_object(
            group="kueue.x-k8s.io",
            version="v1beta1",
            plural="resourceflavors",
            name=flavor_name,
        )
        
        return {
            "name": flavor_name,
            "node_labels": flavor_info.get("spec", {}).get("nodeLabels", {}),
            "tolerations": flavor_info.get("spec", {}).get("tolerations", []),
            "metadata": flavor_info.get("metadata", {}),
            "status": flavor_info.get("status", {})
        }
        
    except ApiException as e:
        return _kube_api_error_handling(e)


def get_cluster_queue_details(cluster_queue_name: str) -> Dict[str, Any]:
    """
    Get detailed information about a cluster queue including resource quotas.
    
    Args:
        cluster_queue_name (str):
            The name of the cluster queue to get details for.
            
    Returns:
        Dict[str, Any]:
            Dictionary containing cluster queue information including:
            - Resource groups and flavors
            - Quotas and borrowing limits
            - Namespace selectors
            - Status information
    """
    try:
        config_check()
        api_instance = client.CustomObjectsApi(get_api_client())
        
        cluster_queue_info = api_instance.get_cluster_custom_object(
            group="kueue.x-k8s.io",
            version="v1beta1",
            plural="clusterqueues",
            name=cluster_queue_name,
        )
        
        return {
            "name": cluster_queue_name,
            "spec": cluster_queue_info.get("spec", {}),
            "status": cluster_queue_info.get("status", {}),
            "metadata": cluster_queue_info.get("metadata", {})
        }
        
    except ApiException as e:
        return _kube_api_error_handling(e)


def get_available_resources_summary(namespace: Optional[str] = None) -> Dict[str, Any]:
    """
    Get a high-level summary of available resources across all queues in a namespace.
    
    This function provides a quick overview of what resources are available to users
    without diving into detailed queue information.
    
    Args:
        namespace (str, optional):
            The namespace to query. Defaults to current namespace.
            
    Returns:
        Dict[str, Any]:
            Summary of available resources including:
            - Total available resources by type
            - Number of queues
            - Default queue information
            - Resource utilization (if available)
    """
    queue_info = get_queue_resource_info(namespace)
    
    # Extract summary information
    summary = {
        "namespace": namespace or get_current_namespace(),
        "total_queues": queue_info["summary"]["total_queues"],
        "default_queue": queue_info["summary"]["default_queue"],
        "available_resources": queue_info["summary"]["available_resources"],
        "queue_names": list(queue_info["queues"].keys()),
        "resource_types": set(),
        "flavors": set()
    }
    
    # Collect resource types and flavors
    for queue_name, queue_data in queue_info["queues"].items():
        for flavor in queue_data["flavors"]:
            summary["flavors"].add(flavor["name"])
            for resource_type in flavor["resources"].keys():
                summary["resource_types"].add(resource_type)
    
    summary["resource_types"] = list(summary["resource_types"])
    summary["flavors"] = list(summary["flavors"])
    
    return summary


def analyze_queue_utilization(namespace: Optional[str] = None) -> Dict[str, Any]:
    """
    Analyze current queue utilization and provide recommendations for optimal resource usage.
    
    This function examines all queues in the namespace and provides:
    - Current utilization statistics for each queue
    - Recommendations for optimal queue selection
    - Warnings about over-utilized queues
    - Suggestions for load balancing
    
    Args:
        namespace (str, optional):
            The namespace to analyze. Defaults to current namespace.
            
    Returns:
        Dict[str, Any]:
            Dictionary containing utilization analysis and recommendations:
            {
                "analysis": {
                    "total_queues": int,
                    "total_workloads": int,
                    "average_utilization": float,
                    "most_utilized_queue": str,
                    "least_utilized_queue": str
                },
                "queues": [
                    {
                        "name": str,
                        "pending": int,
                        "admitted": int,
                        "running": int,
                        "total": int,
                        "is_default": bool,
                        "utilization_level": str,  # "low", "medium", "high", "critical"
                        "recommendation": str
                    }
                ],
                "recommendations": {
                    "primary_suggestion": str,
                    "warnings": List[str],
                    "suggestions": List[str]
                }
            }
    """
    if namespace is None:
        namespace = get_current_namespace()
    
    queue_info = get_queue_resource_info(namespace)
    
    if not queue_info["queues"]:
        return {
            "analysis": {
                "total_queues": 0,
                "total_workloads": 0,
                "average_utilization": 0.0,
                "most_utilized_queue": None,
                "least_utilized_queue": None
            },
            "queues": [],
            "recommendations": {
                "primary_suggestion": "No queues found in namespace",
                "warnings": [],
                "suggestions": ["Check if Kueue is properly configured in this namespace"]
            }
        }
    
    utilization_data = []
    total_workloads = 0
    
    # Analyze each queue
    for queue_name, queue_data in queue_info["queues"].items():
        status = queue_data["status"]
        queue_total = status["pending"] + status["admitted"] + status["running"]
        total_workloads += queue_total
        
        # Determine utilization level
        if queue_total == 0:
            utilization_level = "low"
            recommendation = "✅ Available for new workloads"
        elif queue_total <= 2:
            utilization_level = "low"
            recommendation = "✅ Good capacity for new workloads"
        elif queue_total <= 5:
            utilization_level = "medium"
            recommendation = "⚠️ Moderate utilization - monitor capacity"
        elif queue_total <= 10:
            utilization_level = "high"
            recommendation = "⚠️ High utilization - consider alternatives"
        else:
            utilization_level = "critical"
            recommendation = "🚨 Critical utilization - avoid for new workloads"
        
        utilization_data.append({
            "name": queue_name,
            "pending": status["pending"],
            "admitted": status["admitted"],
            "running": status["running"],
            "total": queue_total,
            "is_default": queue_data["is_default"],
            "utilization_level": utilization_level,
            "recommendation": recommendation
        })
    
    # Sort by utilization (most utilized first)
    utilization_data.sort(key=lambda x: x["total"], reverse=True)
    
    # Calculate analysis metrics
    total_queues = len(utilization_data)
    average_utilization = total_workloads / total_queues if total_queues > 0 else 0
    
    most_utilized = utilization_data[0] if utilization_data else None
    least_utilized = utilization_data[-1] if utilization_data else None
    
    # Generate recommendations
    warnings = []
    suggestions = []
    
    # Check for critical queues
    critical_queues = [q for q in utilization_data if q["utilization_level"] == "critical"]
    if critical_queues:
        warnings.append(f"🚨 {len(critical_queues)} queue(s) have critical utilization: {[q['name'] for q in critical_queues]}")
    
    # Check for high utilization queues
    high_queues = [q for q in utilization_data if q["utilization_level"] == "high"]
    if high_queues:
        warnings.append(f"⚠️ {len(high_queues)} queue(s) have high utilization: {[q['name'] for q in high_queues]}")
    
    # Check for queues with many pending workloads
    pending_heavy = [q for q in utilization_data if q["pending"] > 3]
    if pending_heavy:
        warnings.append(f"⏳ {len(pending_heavy)} queue(s) have many pending workloads: {[q['name'] for q in pending_heavy]}")
    
    # Generate suggestions
    if least_utilized and least_utilized["total"] < average_utilization:
        suggestions.append(f"💡 Consider using '{least_utilized['name']}' for new workloads (lowest utilization)")
    
    if most_utilized and most_utilized["total"] > average_utilization * 2:
        suggestions.append(f"⚖️ '{most_utilized['name']}' is heavily utilized - consider load balancing")
    
    # Check for default queue utilization
    default_queue = next((q for q in utilization_data if q["is_default"]), None)
    if default_queue and default_queue["utilization_level"] in ["high", "critical"]:
        suggestions.append(f"🎯 Default queue '{default_queue['name']}' is over-utilized - consider specifying a different queue")
    
    # Primary suggestion
    if warnings:
        primary_suggestion = f"⚠️ {len(warnings)} issue(s) detected - review queue utilization"
    elif suggestions:
        primary_suggestion = "💡 Optimization opportunities available"
    else:
        primary_suggestion = "✅ All queues have healthy utilization"
    
    return {
        "analysis": {
            "total_queues": total_queues,
            "total_workloads": total_workloads,
            "average_utilization": round(average_utilization, 2),
            "most_utilized_queue": most_utilized["name"] if most_utilized else None,
            "least_utilized_queue": least_utilized["name"] if least_utilized else None
        },
        "queues": utilization_data,
        "recommendations": {
            "primary_suggestion": primary_suggestion,
            "warnings": warnings,
            "suggestions": suggestions
        }
    }


def find_best_queue_for_workload(
    cluster_config: Optional[Any] = None,
    cpu_required: Optional[float] = None,
    memory_required: Optional[float] = None,
    gpu_required: Optional[float] = None,
    namespace: Optional[str] = None,
    avoid_critical: bool = True,
    prefer_default: bool = False,
    verbose: bool = True
) -> Dict[str, Any]:
    """
    Find the best queue and flavor for a workload with specific resource requirements.
    
    This function analyzes all available queues and flavors to find the optimal
    configuration for a workload. It can accept either a ClusterConfiguration object
    or individual resource requirements. It considers queue utilization, resource 
    availability, and user preferences.
    
    Args:
        cluster_config (ClusterConfiguration, optional):
            A ClusterConfiguration object to analyze. If provided, resource requirements
            will be extracted from this configuration. Takes precedence over individual
            resource parameters.
        cpu_required (float, optional):
            Required CPU cores for the workload. Defaults to 1.0 if cluster_config not provided.
        memory_required (float, optional):
            Required memory in GB for the workload. Defaults to 4.0 if cluster_config not provided.
        gpu_required (float, optional):
            Required GPU count for the workload. Defaults to 0.0 if cluster_config not provided.
        namespace (str, optional):
            The namespace to search in. Defaults to current namespace.
        avoid_critical (bool):
            Whether to avoid queues with critical utilization. Defaults to True.
        prefer_default (bool):
            Whether to prefer the default queue when multiple options are available. Defaults to False.
        verbose (bool):
            Whether to print formatted output to console. Defaults to True.
            
    Returns:
        Dict[str, Any]:
            Dictionary containing the best queue recommendation:
            {
                "success": bool,
                "queue_name": str,
                "flavor_name": str,
                "reason": str,
                "available_resources": {
                    "cpu": float,
                    "memory": float,
                    "gpu": float
                },
                "utilization_score": int,
                "is_default": bool,
                "alternatives": [
                    {
                        "queue_name": str,
                        "flavor_name": str,
                        "available_resources": {...},
                        "utilization_score": int,
                        "reason": str
                    }
                ],
                "warnings": List[str],
                "recommendations": List[str],
                "cluster_config_suggestions": {
                    "local_queue": str,
                    "worker_cpu_requests": str,
                    "worker_cpu_limits": str,
                    "worker_memory_requests": int,
                    "worker_memory_limits": int,
                    "worker_extended_resource_requests": dict,
                    "worker_extended_resource_limits": dict
                }
            }
    """
    if namespace is None:
        namespace = get_current_namespace()
    
    # Extract resource requirements from ClusterConfiguration if provided
    if cluster_config is not None:
        # Import ClusterConfiguration here to avoid circular imports
        try:
            from ..ray.cluster.cluster_configuration import ClusterConfiguration
        except ImportError:
            # Fallback for different import paths
            pass
        
        # Extract resource requirements from cluster configuration
        num_workers = getattr(cluster_config, 'num_workers', 1)
        
        # Parse CPU requirements
        worker_cpu_str = getattr(cluster_config, 'worker_cpu_requests', '1')
        if isinstance(worker_cpu_str, str):
            # Convert "500m" to 0.5, "1" to 1.0, etc.
            if worker_cpu_str.endswith('m'):
                cpu_per_worker = float(worker_cpu_str[:-1]) / 1000
            else:
                cpu_per_worker = float(worker_cpu_str)
        else:
            cpu_per_worker = float(worker_cpu_str)
        
        # Parse memory requirements
        worker_memory = getattr(cluster_config, 'worker_memory_requests', 4)
        memory_per_worker = float(worker_memory)
        
        # Parse GPU requirements
        worker_gpu_requests = getattr(cluster_config, 'worker_extended_resource_requests', {})
        gpu_per_worker = float(worker_gpu_requests.get('nvidia.com/gpu', 0))
        
        # Calculate total requirements
        cpu_required = cpu_per_worker * num_workers
        memory_required = memory_per_worker * num_workers
        gpu_required = gpu_per_worker * num_workers
        
        # Override individual parameters if cluster_config is provided
        if verbose:
            print(f"📋 Extracted from ClusterConfiguration:")
            print(f"  Workers: {num_workers}")
            print(f"  CPU per worker: {cpu_per_worker}")
            print(f"  Memory per worker: {memory_per_worker}GB")
            print(f"  GPU per worker: {gpu_per_worker}")
            print(f"  Total CPU required: {cpu_required}")
            print(f"  Total Memory required: {memory_required}GB")
            print(f"  Total GPU required: {gpu_required}")
    else:
        # Use individual parameters with defaults
        cpu_required = cpu_required or 1.0
        memory_required = memory_required or 4.0
        gpu_required = gpu_required or 0.0
    
    queue_info = get_queue_resource_info(namespace)
    
    if not queue_info["queues"]:
        return {
            "success": False,
            "queue_name": None,
            "flavor_name": None,
            "reason": "No queues found in namespace",
            "available_resources": {},
            "utilization_score": 0,
            "is_default": False,
            "alternatives": [],
            "warnings": ["No queues available"],
            "recommendations": ["Check if Kueue is properly configured in this namespace"]
        }
    
    suitable_options = []
    warnings = []
    recommendations = []
    
    # Analyze each queue and flavor
    for queue_name, queue_data in queue_info["queues"].items():
        status = queue_data["status"]
        utilization_score = status["pending"] + status["admitted"] + status["running"]
        
        # Skip critical queues if avoid_critical is True
        if avoid_critical and utilization_score > 10:
            warnings.append(f"Queue '{queue_name}' has critical utilization ({utilization_score} workloads)")
            continue
        
        # Skip queues with too many pending workloads
        if status["pending"] > 5:
            warnings.append(f"Queue '{queue_name}' has {status['pending']} pending workloads")
            continue
        
        for flavor in queue_data["flavors"]:
            resources = flavor["resources"]
            
            # Parse available resources
            cpu_available = float(resources.get("cpu", {}).get("nominalQuota", "0"))
            memory_available = float(resources.get("memory", {}).get("nominalQuota", "0").replace("Gi", ""))
            gpu_available = float(resources.get("nvidia.com/gpu", {}).get("nominalQuota", "0"))
            
            # Check if this flavor meets requirements
            if (cpu_available >= cpu_required and 
                memory_available >= memory_required and 
                gpu_available >= gpu_required):
                
                # Calculate a score (lower is better)
                # Consider utilization, resource headroom, and default preference
                score = utilization_score
                
                # Bonus for default queue if prefer_default is True
                if prefer_default and queue_data["is_default"]:
                    score -= 1
                
                # Bonus for having more resources than required
                cpu_headroom = cpu_available - cpu_required
                memory_headroom = memory_available - memory_required
                gpu_headroom = gpu_available - gpu_required
                
                if cpu_headroom > 0:
                    score -= 0.5
                if memory_headroom > 0:
                    score -= 0.5
                if gpu_headroom > 0:
                    score -= 0.5
                
                suitable_options.append({
                    "queue_name": queue_name,
                    "flavor_name": flavor["name"],
                    "available_resources": {
                        "cpu": cpu_available,
                        "memory": memory_available,
                        "gpu": gpu_available
                    },
                    "utilization_score": utilization_score,
                    "is_default": queue_data["is_default"],
                    "score": score,
                    "reason": f"Meets requirements with {cpu_headroom:.1f} CPU, {memory_headroom:.1f}GB memory, {gpu_headroom:.0f} GPU headroom"
                })
    
    if not suitable_options:
        failure_result = {
            "success": False,
            "queue_name": None,
            "flavor_name": None,
            "reason": f"No suitable queues found for requirements: {cpu_required} CPU, {memory_required}GB memory, {gpu_required} GPU",
            "available_resources": {},
            "utilization_score": 0,
            "is_default": False,
            "alternatives": [],
            "warnings": warnings + [f"No queues meet the resource requirements"],
            "recommendations": [
                "Consider reducing resource requirements",
                "Check if additional queues are available",
                "Wait for current workloads to complete"
            ],
            "cluster_config_suggestions": {
                "local_queue": None,
                "worker_cpu_requests": f"{cpu_required/2:.1f}",
                "worker_cpu_limits": f"{cpu_required/2:.1f}",
                "worker_memory_requests": int(memory_required//2),
                "worker_memory_limits": int(memory_required//2),
                "worker_extended_resource_requests": {"nvidia.com/gpu": int(gpu_required)} if gpu_required > 0 else {},
                "worker_extended_resource_limits": {"nvidia.com/gpu": int(gpu_required)} if gpu_required > 0 else {}
            }
        }
        
        # Print formatted output if verbose is True
        if verbose:
            _print_queue_selection_result(failure_result, cpu_required, memory_required, gpu_required)
        
        return failure_result
    
    # Sort by score (best first)
    suitable_options.sort(key=lambda x: x["score"])
    
    best_option = suitable_options[0]
    alternatives = suitable_options[1:5]  # Top 5 alternatives
    
    # Generate recommendations
    if len(suitable_options) > 1:
        recommendations.append(f"Found {len(suitable_options)} suitable options")
        recommendations.append(f"Best option: {best_option['queue_name']} (utilization: {best_option['utilization_score']})")
        
        if best_option["is_default"]:
            recommendations.append("Using default queue - no explicit queue specification needed")
        else:
            recommendations.append(f"Specify local_queue='{best_option['queue_name']}' in ClusterConfiguration")
    else:
        recommendations.append("Only one suitable option found")
    
    # Add resource-specific recommendations
    if cpu_required > 2:
        recommendations.append("Consider using multiple workers for CPU-intensive workloads")
    
    if memory_required > 16:
        recommendations.append("High memory requirement - ensure sufficient cluster capacity")
    
    if gpu_required > 0:
        recommendations.append("GPU workload detected - verify GPU availability and drivers")
    
    # Generate cluster configuration suggestions
    cluster_config_suggestions = {
        "local_queue": best_option["queue_name"] if not best_option["is_default"] else None,
        "worker_cpu_requests": f"{cpu_required/2:.1f}",
        "worker_cpu_limits": f"{cpu_required/2:.1f}",
        "worker_memory_requests": int(memory_required//2),
        "worker_memory_limits": int(memory_required//2),
        "worker_extended_resource_requests": {"nvidia.com/gpu": int(gpu_required)} if gpu_required > 0 else {},
        "worker_extended_resource_limits": {"nvidia.com/gpu": int(gpu_required)} if gpu_required > 0 else {}
    }
    
    result = {
        "success": True,
        "queue_name": best_option["queue_name"],
        "flavor_name": best_option["flavor_name"],
        "reason": best_option["reason"],
        "available_resources": best_option["available_resources"],
        "utilization_score": best_option["utilization_score"],
        "is_default": best_option["is_default"],
        "alternatives": alternatives,
        "warnings": warnings,
        "recommendations": recommendations,
        "cluster_config_suggestions": cluster_config_suggestions
    }
    
    # Print formatted output if verbose is True
    if verbose:
        _print_queue_selection_result(result, cpu_required, memory_required, gpu_required)
    
    return result


def _print_queue_selection_result(result: Dict[str, Any], cpu_required: float, memory_required: float, gpu_required: float) -> None:
    """
    Print formatted output for queue selection results.
    
    Args:
        result: The result dictionary from find_best_queue_for_workload
        cpu_required: Required CPU cores
        memory_required: Required memory in GB
        gpu_required: Required GPU count
    """
    print("=" * 60)
    print("🎯 QUEUE SELECTION RESULT")
    print("=" * 60)
    
    # Print workload requirements
    print(f"\n📋 Workload Requirements:")
    print(f"  CPU: {cpu_required} cores")
    print(f"  Memory: {memory_required}GB")
    print(f"  GPU: {gpu_required}")
    
    if result['success']:
        print(f"\n✅ RECOMMENDED CONFIGURATION:")
        print(f"  Queue: {result['queue_name']}")
        print(f"  Flavor: {result['flavor_name']}")
        print(f"  Available Resources:")
        print(f"    CPU: {result['available_resources']['cpu']} cores")
        print(f"    Memory: {result['available_resources']['memory']}GB")
        print(f"    GPU: {result['available_resources']['gpu']}")
        print(f"  Queue Utilization: {result['utilization_score']} active workloads")
        print(f"  Is Default Queue: {'Yes' if result['is_default'] else 'No'}")
        print(f"  Reason: {result['reason']}")
        
        # Show recommendations
        if result['recommendations']:
            print(f"\n💡 RECOMMENDATIONS:")
            for i, rec in enumerate(result['recommendations'], 1):
                print(f"  {i}. {rec}")
        
        # Show alternatives
        if result['alternatives']:
            print(f"\n🔄 ALTERNATIVE OPTIONS:")
            for i, alt in enumerate(result['alternatives'][:3], 1):
                print(f"  {i}. {alt['queue_name']} ({alt['flavor_name']})")
                print(f"     Utilization: {alt['utilization_score']} workloads")
                print(f"     Resources: {alt['available_resources']['cpu']} CPU, {alt['available_resources']['memory']}GB RAM, {alt['available_resources']['gpu']} GPU")
        
        # Show warnings if any
        if result['warnings']:
            print(f"\n⚠️ WARNINGS:")
            for warning in result['warnings']:
                print(f"  - {warning}")
        
        # Show cluster configuration example
        print(f"\n🚀 CLUSTER CONFIGURATION EXAMPLE:")
        print(f"  cluster_config = ClusterConfiguration(")
        print(f"      name='my-cluster',")
        
        # Use cluster_config_suggestions if available, otherwise fallback to calculated values
        if 'cluster_config_suggestions' in result:
            suggestions = result['cluster_config_suggestions']
            if suggestions['local_queue']:
                print(f"      local_queue='{suggestions['local_queue']}',")
            print(f"      num_workers=2,")
            print(f"      worker_cpu_requests='{suggestions['worker_cpu_requests']}',")
            print(f"      worker_cpu_limits='{suggestions['worker_cpu_limits']}',")
            print(f"      worker_memory_requests={suggestions['worker_memory_requests']},")
            print(f"      worker_memory_limits={suggestions['worker_memory_limits']},")
            if suggestions['worker_extended_resource_requests']:
                print(f"      worker_extended_resource_requests={suggestions['worker_extended_resource_requests']},")
                print(f"      worker_extended_resource_limits={suggestions['worker_extended_resource_limits']},")
        else:
            # Fallback to original logic
            if not result['is_default']:
                print(f"      local_queue='{result['queue_name']}',")
            print(f"      num_workers=2,")
            print(f"      worker_cpu_requests='{cpu_required/2:.1f}',")
            print(f"      worker_cpu_limits='{cpu_required/2:.1f}',")
            print(f"      worker_memory_requests={memory_required//2},")
            print(f"      worker_memory_limits={memory_required//2},")
            if gpu_required > 0:
                print(f"      worker_extended_resource_requests={{'nvidia.com/gpu': {gpu_required}}},")
                print(f"      worker_extended_resource_limits={{'nvidia.com/gpu': {gpu_required}}},")
        
        print(f"  )")
        
    else:
        print(f"\n❌ NO SUITABLE QUEUE FOUND")
        print(f"  Reason: {result['reason']}")
        
        if result['warnings']:
            print(f"\n⚠️ WARNINGS:")
            for warning in result['warnings']:
                print(f"  - {warning}")
        
        if result['recommendations']:
            print(f"\n💡 RECOMMENDATIONS:")
            for i, rec in enumerate(result['recommendations'], 1):
                print(f"  {i}. {rec}")
    
    print("=" * 60)
