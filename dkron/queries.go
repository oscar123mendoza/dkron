package dkron

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/hashicorp/serf/serf"
	"github.com/sirupsen/logrus"
)

const (
	QuerySchedulerRestart = "scheduler:restart"
	QueryRunJob           = "run:job"
	QueryExecutionDone    = "execution:done"
)

var rescheduleThrotle *time.Timer

type RunQueryParam struct {
	Execution *Execution `json:"execution"`
	RPCAddr   string     `json:"rpc_addr"`
}

// Send a serf run query to the cluster, this is used to ask a node or nodes
// to run a Job.
func (a *Agent) RunQuery(ex *Execution) {
	var params *serf.QueryParam

	job, err := a.Store.GetJob(ex.JobName, nil)

	if err != nil {
		//Job can be removed and the QuerySchedulerRestart not yet received.
		//In this case, the job will not be found in the store.
		if err == badger.ErrKeyNotFound {
			log.Warning("agent: Job not found, cancelling this execution")
			return
		}
		log.WithError(err).Fatal("agent: Getting job error")
		return
	}

	// In the first execution attempt we build and filter the target nodes
	// but we use the existing node target in case of retry.
	if ex.Attempt <= 1 {
		filterNodes, filterTags, err := a.processFilteredNodes(job)
		if err != nil {
			log.WithFields(logrus.Fields{
				"job": job.Name,
				"err": err.Error(),
			}).Fatal("agent: Error processing filtered nodes")
		}
		log.Debug("agent: Filtered nodes to run: ", filterNodes)
		log.Debug("agent: Filtered tags to run: ", job.Tags)

		//serf match regexp but we want only match full tag
		serfFilterTags := make(map[string]string)
		for key, val := range filterTags {
			b := new(bytes.Buffer)
			b.WriteString("^")
			b.WriteString(val)
			b.WriteString("$")
			serfFilterTags[key] = b.String()
		}

		params = &serf.QueryParam{
			FilterNodes: filterNodes,
			FilterTags:  serfFilterTags,
			RequestAck:  true,
		}
	} else {
		params = &serf.QueryParam{
			FilterNodes: []string{ex.NodeName},
			RequestAck:  true,
		}
	}

	rqp := &RunQueryParam{
		Execution: ex,
		RPCAddr:   a.getRPCAddr(),
	}
	rqpJson, _ := json.Marshal(rqp)

	log.WithFields(logrus.Fields{
		"query":    QueryRunJob,
		"job_name": job.Name,
	}).Info("agent: Sending query")

	log.WithFields(logrus.Fields{
		"query":    QueryRunJob,
		"job_name": job.Name,
		"json":     string(rqpJson),
	}).Debug("agent: Sending query")

	qr, err := a.serf.Query(QueryRunJob, rqpJson, params)
	if err != nil {
		log.WithField("query", QueryRunJob).WithError(err).Fatal("agent: Sending query error")
	}
	defer qr.Close()

	ackCh := qr.AckCh()
	respCh := qr.ResponseCh()

	for !qr.Finished() {
		select {
		case ack, ok := <-ackCh:
			if ok {
				log.WithFields(logrus.Fields{
					"query": QueryRunJob,
					"from":  ack,
				}).Debug("agent: Received ack")
			}
		case resp, ok := <-respCh:
			if ok {
				log.WithFields(logrus.Fields{
					"query":    QueryRunJob,
					"from":     resp.From,
					"response": string(resp.Payload),
				}).Debug("agent: Received response")

				// Save execution to store
				a.setExecution(resp.Payload)
			}
		}
	}
	log.WithFields(logrus.Fields{
		"query": QueryRunJob,
	}).Debug("agent: Done receiving acks and responses")
}

// Broadcast a ExecutionDone to the cluster.
func (a *Agent) executionDoneQuery(nodes []string, group string) map[string]string {
	params := &serf.QueryParam{
		FilterNodes: nodes,
		RequestAck:  true,
	}

	log.WithFields(logrus.Fields{
		"query":   QueryExecutionDone,
		"members": nodes,
	}).Info("agent: Sending query")

	qr, err := a.serf.Query(QueryExecutionDone, []byte(group), params)
	if err != nil {
		log.WithError(err).Fatal("agent: Error sending the execution done query")
	}
	defer qr.Close()

	statuses := make(map[string]string)
	ackCh := qr.AckCh()
	respCh := qr.ResponseCh()

	for !qr.Finished() {
		select {
		case ack, ok := <-ackCh:
			if ok {
				log.WithFields(logrus.Fields{
					"from": ack,
				}).Debug("agent: Received ack")
			}
		case resp, ok := <-respCh:
			if ok {
				log.WithFields(logrus.Fields{
					"from":    resp.From,
					"payload": string(resp.Payload),
				}).Debug("agent: Received response")

				statuses[resp.From] = string(resp.Payload)
			}
		}
	}
	log.WithField("query", QueryExecutionDone).Debug("agent: Done receiving acks and responses")

	// In case the query finishes by deadline without receiving a response from the node
	// set the execution as finished, maybe the node is gone by now.
	return statuses
}
