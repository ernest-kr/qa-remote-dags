# How To Run The Dags Here
Some of the dags need special settings before they can run successfully.

## k8s_private_image_pod.py
To run this dag, you need to build a private image and publish it to your registry

You can follow this steps to build an image and push to private repository if you don't
have a private image to use.

```bash
mkdir privateimage && cd privateimage
```
Create a Dockerfile and fill it with this:
```dockerfile
FROM ubuntu:18.04

```
Now build the image:
```bash
docker build -t yourusername/privateimage .
```
Please don't forget the dot at the end of the above command

Push the image to your repository:
```bash
docker push yourusername/privateimage
```

Navigate to your registry and make the repository private.

Now create a secret in your command line for kubernetes
```bash
kubectl create secret generic mysecret --from-file=.dockerconfigjson=/home/yourusername/.docker/config.json --type=kubernestes.io/dockerconfigjson
```
I'm assuming above that your `.docker/config.json` is in the root of your computer
otherwise, find it and specify path to it.

Now open the example dag `k8s_private_image_pod.py` and change the pod image
to your image.

##k8s_pod_use_secret.py
To be able to get this example dag to run, you must create 2 kubernetes secrets.
The secrets must be named `airflow-secrets-1` and `airflow-secrets-2`.
Here's how to create these secrets:
Create `airflow-secrets-1`:
```
kubectl create secret generic airflow-secrets-1 --from-literal sql_alchemy_conn=test_value
```
And for `airflow-secrets-2`:
```
kubectl create secret generic airflow-secrets-2 --from-literal sql_alchemy_conn=test_value
```
Check that the secrets exists in your cluster:
```
kubectl get secrets
```

##k8_pod_volume_mount.py
For this example dag to succeed successfully, you must create two configmaps.
Here are commands to run:
Create `test-config-map-1`:
```
kubectl create configmap test-configmap-1 --from-literal foo=bar
```
Create `test-config-map-2`:
```
kubectl create configmap test-configmap-2 --from-literal foo=bar
```
Now run the example dag and it should succeed.
