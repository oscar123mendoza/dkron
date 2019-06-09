---
title: Clustering
---

## Configure a cluster

Dkron can run in HA mode, avoiding SPOFs, this mode provides better scalability and better reliability for users that wants a high level of confidence in the cron jobs they need to run.

To form a cluster, server nodes need to know the address of its peers as in the following example:

```yaml
# dkron.yml
bootstrap-expect: 3
join:
- 10.19.3.9
- 10.19.4.64
- 10.19.7.215
```
