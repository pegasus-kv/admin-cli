package nodesmigrator

import (
	"fmt"
	"sync"
)

type Action struct {
	from    *MigratorNode
	to      *MigratorNode
	replica *Replica
}

type MigrateActions struct {
	actionList map[string]*Action
}

var migrateActionsMu sync.Mutex

func (acts *MigrateActions) put(currentAction *Action) bool {
	migrateActionsMu.Lock()
	defer func() {
		migrateActionsMu.Unlock()
	}()

	if acts.exist(currentAction) {
		return false
	}

	acts.actionList[currentAction.toString()] = currentAction
	return true
}

func (acts *MigrateActions) exist(currentAction *Action) bool {
	for _, action := range acts.actionList {
		if action.replica.part.Pid.String() == currentAction.replica.part.Pid.String() {
			if action.to.node.String() == currentAction.to.node.String() {
				fmt.Printf("WARN: has called the action: %s, current: %s\n", action.toString(), currentAction.toString())
				return true
			}
		}
	}
	return false
}

func (act *Action) toString() string {
	return fmt.Sprintf("[%s]%s:%s=>%s", act.replica.operation.String(), act.replica.part.Pid.String(),
		act.from.node.String(), act.to.node.String())
}
