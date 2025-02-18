using System;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.Models;

namespace K8sWatcher
{
    class Program {
        const string RebootAnnotation = "reboot-agent.v1.sdlt.local/reboot";
        const string RebootInProgressAnnotation = "reboot-agent.v1.sdlt.local/reboot-in-progress";

        static async Task Main(string[] args) {
            var config = KubernetesClientConfiguration.BuildConfigFromConfigFile();
            var client = new Kubernetes(config);
            // var config = KubernetesClientConfiguration.InClusterConfig();
            
            var listResp = await client.CoreV1.ListPodForAllNamespacesWithHttpMessagesAsync(watch: true);
            using (listResp.Watch<V1Pod, V1PodList>((type, item) =>
            {
                switch (type)
                {
                    case WatchEventType.Added:
                        Console.WriteLine($"Pod added: {item.Metadata.Name}");
                        break;

                    case WatchEventType.Modified:
                        Console.WriteLine($"Pod modified: {item.Metadata.Name}");

                        if (item.Metadata.Annotations != null)
                        {
                            HandleAnnotations(item, client);
                        }
                        break;

                    case WatchEventType.Deleted:
                        Console.WriteLine($"Pod deleted: {item.Metadata.Name}");
                        break;

                    default:
                        Console.WriteLine($"Unknown event type: {type}");
                        break;
                }
            }))
            {
                Console.WriteLine("Press Ctrl+C to exit.");
                await Task.Delay(-1);
            }
        }

        static async void HandleAnnotations(V1Pod pod, Kubernetes client)
        {
            var annotations = pod.Metadata.Annotations;
            if (annotations == null)
            {
                return;
            }

            if (annotations.ContainsKey(RebootAnnotation))
            {
                Console.WriteLine($"Reboot annotation found on pod {pod.Metadata.Name}. Restarting deployment.");
                await RestartDeployment(pod, client);
            }
            else if (annotations.ContainsKey(RebootInProgressAnnotation))
            {
                Console.WriteLine($"Reboot in progress annotation found on pod {pod.Metadata.Name}.");
            }
        }

        static async Task RestartDeployment(V1Pod pod, Kubernetes client)
        {
            foreach (var ownerRef in pod.Metadata.OwnerReferences)
            {
                if (ownerRef.Kind == "ReplicaSet")
                {
                    var replicaSet = await client.AppsV1.ReadNamespacedReplicaSetAsync(ownerRef.Name, pod.Metadata.NamespaceProperty);
                    foreach (var rsOwnerRef in replicaSet.Metadata.OwnerReferences)
                    {
                        if (rsOwnerRef.Kind == "Deployment")
                        {
                            int maxRetries = 3;
                            int retryCount = 0;
                            TimeSpan delay = TimeSpan.FromSeconds(10);

                            while (retryCount < maxRetries)
                            {
                                try
                                {
                                    var deployment = await client.AppsV1.ReadNamespacedDeploymentAsync(rsOwnerRef.Name, pod.Metadata.NamespaceProperty);

                                    // Initialize the annotations map if it's null
                                    if (deployment.Spec.Template.Metadata.Annotations == null)
                                    {
                                        deployment.Spec.Template.Metadata.Annotations = new System.Collections.Generic.Dictionary<string, string>();
                                    }

                                    // Patch the deployment to trigger a restart
                                    deployment.Spec.Template.Metadata.Annotations["kubectl.kubernetes.io/restartedAt"] = DateTime.UtcNow.ToString("o");
                                    await client.AppsV1.ReplaceNamespacedDeploymentAsync(deployment, deployment.Metadata.Name, pod.Metadata.NamespaceProperty);

                                    Console.WriteLine($"Deployment {deployment.Metadata.Name} restarted.");
                                    break; // Exit the loop if successful
                                }
                                catch (k8s.Autorest.HttpOperationException e) when (e.Response.StatusCode == System.Net.HttpStatusCode.Conflict)
                                {
                                    // Conflict error, increment retry count and wait before retrying
                                    retryCount++;
                                    Console.WriteLine($"Conflict error when updating deployment, retrying in {delay.TotalSeconds} seconds (Attempt {retryCount}/{maxRetries}): {e.Message}");
                                    await Task.Delay(delay);
                                    delay = delay + TimeSpan.FromSeconds(5); // Incremental backoff
                                }
                            }

                            if (retryCount == maxRetries)
                            {
                                Console.WriteLine("Max retries reached. Could not update the deployment.");
                            }
                        }
                    }
                }
            }
        }
    }
}