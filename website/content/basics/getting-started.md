---
title: Getting started
weight: 30
---

## Introduction

Dkron nodes can work in two modes, agents or servers.

A Dkron agent is a cluster member that can handle job executions, run your scripts and return the resulting output to the server.

A Dkron server is also a cluster member that send job execution queries to agents or other servers, so servers can execute jobs too.

The main distinction is that servers order job executions, can be used to schedule jobs, handles data storage and participate on leader election.

Dkron clusters have a leader, the leader is responsible of starting job execution queries in the cluster.

Any Dkron agent or server acts as a cluster member and it's available to run scheduled jobs.

You can choose whether a job is run on a node or nodes by specifying tags and a count of target nodes having this tag do you want a job to run. This gives an unprecedented level of flexibility in runnig jobs across a cluster of any size and with any combination of machines you need.

All the execution responses will be gathered by the scheduler and stored in the database.

## State storage

Dkron deployment is just a single binary, it stores the state in an internal BargerDB instance and replicate all changes between all server nodes using the Raft protocol, it doesn't need any other storage system outside itself.

## Installation

See the [installation](/basics/installation).

## Configuration

See the [configuration](/basics/configuration).

## Usage

By default Dkron uses the following ports:

- `8946` for communicating between agents
- `8080` for HTTP for the API and Dashboard
- `6868` for RPC comunication between agents.

{{% notice note %}}
Be sure you have opened this ports (or the ones that you configured) in your firewall or AWS security groups.
{{% /notice %}}

To start a Dkron server instance:

```
dkron agent --server
```

Time to add the first job:

{{% notice note %}}
This job will only run in just one `server` node due to the node count in the tag. Refer to the [target node spec](/usage/target-nodes-spec) for details.
{{% /notice %}}

```bash
curl localhost:8080/v1/jobs -XPOST -d '{
  "name": "job1",
  "schedule": "@every 10s",
  "timezone": "Europe/Berlin",
  "owner": "Platform Team",
  "owner_email": "platform@example.com",
  "disabled": false,
  "tags": {
    "server": "true:1"
  },
  "concurrency": "allow",
  "executor": "shell",
  "executor_config": {
    "command": "date"
  }
}`
```

That's it!

#### To start configuring an HA installation of Dkron follow the [clustering guide](/usage/clustering)
