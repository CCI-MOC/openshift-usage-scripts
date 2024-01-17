# openshift-usage-scripts

openshift-usage-scripts contains 2 scripts:

1. `openshift-usage-scripts/openshift-prometheus_metrics.py` that collects metrics from prometheus and writes those to a json file.
2. `openshift-usage-scripts/merge.py` takes the collected metrics and produces CSV OpenShift usage reports, one by namespace and other by pod.

This was a fork of https://github.com/OCP-on-NERC/xdmod-openshift-scripts

## Usage

In order to run the scripts, you need to run `oc login` or set the following environment variables:

- `OPENSHIFT_TOKEN` for both scripts.
- `OPENSHIFT_API_URL=https://api.shift.nerc.mghpcc.org:6443` for `merge.py`.

We are using the token for `xdmod-reader` service account in `xdmod-reader` namespace on nerc-prod cluster. You can extract the token with:
`oc get secrets -n xdmod-reader --as system:admin xdmod-reader-token-m6s2m -o yaml | yq .data.token -r |base64 -d` .

Note that if you are logged in with `oc login` then you don't need to manually set the OPENSHIFT_TOKEN or OPENSHIFT_API_URL.

When running the scripts, there are two methods of specifying the OpenShift Prometheus
endpoint. The first is through an environment variable:

```
    $ export OPENSHIFT_PROMETHEUS_URL=<prometheus url>
    $ python openshift-usage-scripts/openshift_prometheus_metrics.py
```

The second is directly on the command line:

```
    $ python openshift-usage-scripts/openshift_prometheus_metrics.py --openshift-url <prometheus url>
```

### Collecting metrics

By default the script will pull data from the day before.

```
   $ python openshift_metrics/openshift_prometheus_metrics.py \
    --openshift-url https://thanos-querier-openshift-monitoring.apps.shift.nerc.mghpcc.org \
```

You can specify a data range to collect metrics for a time period like this:

```
    $ python openshift_metrics/openshift_prometheus_metrics.py \
    --openshift-url https://thanos-querier-openshift-monitoring.apps.shift.nerc.mghpcc.org \
    --report-start-date 2022-03-01 \
    --report-end-date 2022-03-07 \
```

This will collect metrics from March 1st to March 7th, inclusive.

### Merging and producing the report

You can generate the openshift usage report by passing it multiple metrics files

```
   $ python openshift_metrics/merge.py \
     metrics-2024-01-01-to-2024-01-07.json \
     metrics-2024-01-08-to-2024-01-14.json \
     metrics-2024-01-15-to-2024-01-16.json
```

This will merge the metrics and produce the openshift usage report of the period January 1st to January 16.

Output file name can be specified with the `--output-file` flags. You can also pass in a bunch of files like this:

```
$ python openshift_metrics/merge.py data_2024_01/*.json
```

## How It Works

The `openshift_prometheus_metrics.py` retrieves metrics at a pod level. It does so with the
following Prometheus query:

```
   <prometheus_url>/api/v1/query_range?query=<metric>&start=<report_date>T00:00:00Z&end=<report_date>T23:59:59Z&step=<step_min>m
```

This query generates samples for "step_min" minutes. The script will then merge consecutive samples
together if their metrics are the same.

The script queries the following metrics:

* *kube_pod_resource_request{unit="cores"} unless on(pod, namespace) kube_pod_status_unschedulable*
   * Cores requested by a pods that are scheduled to run.
* *'kube_pod_resource_request{unit="bytes"} unless on(pod, namespace) kube_pod_status_unschedulable'*
   * Memory (RAM) requested by pods that are scheduled to run.
* *'kube_pod_resource_request{resource=~".*gpu.*"} unless on(pod, namespace) kube_pod_status_unschedulable'*
   * GPU Requested by pods that are sheculed to run. The requested GPU resource must have the word "gpu" in it
   to be captured by this query. E.g. `nvidia.com/gpu`

The script also retrieves further information through annotations.
