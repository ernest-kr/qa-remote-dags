# Spin Kubernetes Clusters with kind
If you wish to run local kubernetes clusters, below is the steps you need
to take to have your clusters running.

## Prerequisite Installations
Below are some installations you need before you can set up a cluster
### Install kind
Before you can install kind, make sure to install `Go`.
Here's a link for `Go` installation: https://golang.org/

Install kind by running this command:
```bash
GO111MODULE="on" go get sigs.k8s.io/kind@v0.9.0
```
Add the following to your bash profile if they don't exist:
```
export PATH=$PATH:/usr/local/go/bin
export PATH=$PATH:$(go env GOPATH)/bin
```
You can do that in ubuntu by running `vim ~/.bashrc`
Make sure to source after adding the lines above with the command: `source ~/.bashrc`

### Install helm
Visit https://helm.sh/docs/intro/install/ and install helm.

After installation, add airflow to the repository
```bash
helm repo add airflow https://internal-helm.astronomer.io
```
## Create a kind cluster
To start, let's create a kind cluster
```bash
kind create cluster --name airflow
```
If you didn't specify a name, kind will create a cluster called `kind`, please specify a name to
ensure that you don't make mistakes.

## Create kubernetes namespace and set kind cluster to use it
Run this command to create a namespace
```
kubectl create namespace airflow
```
You just created a namespace called airflow. Now set kind cluster to use the namespace.
Without this command below, you will have to always specify the namespace option in other
commands like `-n airflow`
```
kubectl config set-context --current --namespace=airflow
```
## Install airflow in the cluster using helm chart
Here, we are going to use the airflow-chart repository to install airflow in the cluster
Clone the repo and change directory to airflow-chart:
```
git clone git@github.com:astronomer/airflow-chart.git

cd airflow-chart
```
Open `values.yaml`, a file inside the airflow-chart and change the following to match the
image you want to use e.g for the image `quay.io/astronomer/ap-airflow-dev:2.0.0-buster-onbuild-22919 `
I will change it to this:
```yaml
# Airflow version
airflowVersion: 2.0.0

# Default airflow repository
defaultAirflowRepository: quay.io/astronomer/ap-airflow-dev

# Default airflow tag to deploy
defaultAirflowTag: 2.0.0-buster-onbuild-22919

# Astronomer Airflow images
images:
  airflow:
    repository: quay.io/astronomer/ap-airflow-dev
    tag: 2.0.0-buster-onbuild-22919
```
Use this command to install airflow using the helm chart.
```
helm install airflow -f values.yaml . --timeout=2h
```
Please specify a large timeout so the installation don't fail. When the installation fails,
it's difficult to sign in.
Also make sure to run the above command inside `airflow-chart` repository.

When the installation is done, you will receive an output on next step. Typically, you need
to run this command to see the webserver:
```
kubectl port-forward svc/airflow-webserver 8080:8080
```

## Loading dags into your installation
If you have an astro project with dags that you want to deploy to the cluster then build the
image and load it to kind then upgrade your cluster through helm.
The steps are as follows, assuming you don't have an astro project, if you have, move to
building the docker image of the project.
### Create astro project
```
mkdir mynewproject && cd mynewproject
```
Initialize it and copy dags to the dag folder
```
astro dev init
```
### Build image of the project
`yourusername` should be your docker username
```
docker build -t yourusername/mynewproject:0.1-dev .
```
Don't forget to include the dot

### load the image into kind cluster
You must specify the name option otherwise it will be loaded into a cluster named `kind` if it exists
```
kind load docker-image yourusername/mynewproject:0.1-dev --name airflow
```
### Upgrade the airflow installation using the helm chart
This command must be run in airflow-chart repository which is specified with the dot
```
helm upgrade airflow --set images.airflow.repository=yourusername/mynewproject \
    --set images.airflow.tag=0.1-dev .
```
The output from the above command should be similar to
```
Release "airflow" has been upgraded. Happy Helming!
NAME: airflow
LAST DEPLOYED: Sun Nov 22 09:21:57 2020
NAMESPACE: airflow
STATUS: deployed
REVISION: 4
TEST SUITE: None
NOTES:
Thank you for installing Airflow!

Your release is named airflow.

You can now access your dashboard(s) by executing the following command(s) and visiting the corresponding port at localhost in your browser:

Airflow dashboard:        kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airflow

```
Stop your port forwarding if it's still running and re-forward it again. You may need to wait
a bit before forwarding the port.
If you receive an error after forwarding, check the pods to see if they are running fine.
You can check pods with `kubectl get pods`
