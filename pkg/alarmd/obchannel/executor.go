// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package obchannel

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/roles"
)

// ExecutorOptions describes one local evidence provider. HTTP authorization is
// deliberately absent: the internal RPC authenticates the registered caller.
type ExecutorOptions struct {
	EnvironmentID, Replica, Build, Incarnation string
	Concurrency                                int
	Operations                                 []Operation
	Now                                        func() time.Time
	Roles                                      roles.Set
}

// EvidenceExecutor owns registration, validation and the local execution slots
// shared by HTTP and internal RPC. Constructing it performs no evidence I/O.
type EvidenceExecutor struct {
	options   ExecutorOptions
	ops       map[string]Operation
	ordered   []string
	revision  string
	contracts map[string]string
	slots     chan struct{}
}

func NewEvidenceExecutor(options ExecutorOptions) (*EvidenceExecutor, error) {
	if options.EnvironmentID == "" {
		return nil, errors.New("OB evidence executor requires environment identity")
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.Concurrency <= 0 {
		options.Concurrency = 1
	}
	if options.Concurrency > 4 {
		options.Concurrency = 4
	}
	e := &EvidenceExecutor{options: options, ops: make(map[string]Operation), contracts: make(map[string]string), slots: make(chan struct{}, options.Concurrency)}
	for _, op := range options.Operations {
		if op.ID == "" || op.Run == nil || op.Summary == "" {
			return nil, errors.New("invalid OB operation registration")
		}
		if _, found := e.ops[op.ID]; found {
			return nil, fmt.Errorf("duplicate OB operation %q", op.ID)
		}
		if op.ContractVersion == "" {
			op.ContractVersion = "1"
		}
		if op.EvidenceScope == "" {
			op.EvidenceScope = "deployment"
		}
		if op.Targetable {
			for name := range targetFields() {
				if _, found := op.Fields[name]; found {
					return nil, fmt.Errorf("operation %q uses reserved targeting field %q", op.ID, name)
				}
			}
		}
		if op.DefaultOwnerParam != "" {
			field, exists := op.Fields[op.DefaultOwnerParam]
			required := false
			for _, name := range op.Required {
				required = required || name == op.DefaultOwnerParam
			}
			if !op.Targetable || !exists || field.Type != "string" || !required {
				return nil, fmt.Errorf("invalid default owner field for %q", op.ID)
			}
		}
		for _, name := range op.Required {
			if _, ok := op.Fields[name]; !ok {
				return nil, fmt.Errorf("unknown required field %q", name)
			}
		}
		e.ops[op.ID] = op
		e.ordered = append(e.ordered, op.ID)
		contract := describe(op)
		// Catalog presentation and examples do not change execution semantics.
		for _, key := range []string{"summary", "examples", "time_semantics"} {
			delete(contract, key)
		}
		digest, err := contractDigest(contract)
		if err != nil {
			return nil, err
		}
		e.contracts[op.ID] = digest
	}
	sort.Strings(e.ordered)
	catalog := make([]any, 0, len(e.ordered))
	for _, id := range e.ordered {
		catalog = append(catalog, describe(e.ops[id]))
	}
	var err error
	e.revision, err = contractDigest(catalog)
	return e, err
}

func contractDigest(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func (e *EvidenceExecutor) OperationContractRevision(operation string) string {
	return e.contracts[operation]
}

// AllowsTargetedOperation is the internal dispatch allow-list. Unregistered
// and deployment-only operations are rejected before routing or execution.
func (e *EvidenceExecutor) AllowsTargetedOperation(operation string) bool {
	op, found := e.ops[operation]
	return found && (op.Targetable || op.DefaultControlLeader)
}
