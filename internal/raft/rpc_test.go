package raft

import (
	"encoding/json"
	"testing"
)

func TestRPCMarshalRoundTrip(t *testing.T) {

	req := RequestVoteRequest{
		Term:         5,
		CandidateID:  "node-2",
		LastLogIndex: 10,
		LastLogTerm:  4,
	}

	data, err := json.Marshal(req)

	if err != nil {
		t.Fatal(err)
	}

	var back RequestVoteRequest

	err = json.Unmarshal(
		data,
		&back,
	)

	if err != nil {
		t.Fatal(err)
	}

	if back.Term != 5 ||
		back.CandidateID != "node-2" ||
		back.LastLogIndex != 10 ||
		back.LastLogTerm != 4 {

		t.Fatalf(
			"round trip mismatch: %+v",
			back,
		)
	}

	aReq := AppendEntriesRequest{

		Term: 5,

		LeaderID: "node-2",

		PrevLogIndex: 9,

		PrevLogTerm: 4,

		Entries: []LogEntry{
			{
				Index: 10,
				Term:  5,

				Command: Command{
					Type: "message",
				},
			},
		},

		LeaderCommit: 0,
	}

	data2, err := json.Marshal(aReq)

	if err != nil {
		t.Fatal(err)
	}

	var aBack AppendEntriesRequest

	err = json.Unmarshal(
		data2,
		&aBack,
	)

	if err != nil {
		t.Fatal(err)
	}

	if len(aBack.Entries) != 1 ||
		aBack.Entries[0].Index != 10 ||
		aBack.Entries[0].Term != 5 {

		t.Fatalf(
			"append mismatch: %+v",
			aBack,
		)
	}
}
