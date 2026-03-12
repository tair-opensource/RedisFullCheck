package common

import "testing"

func TestParseClusterNodeRole(t *testing.T) {
	content := []byte("" +
		"node1 11.0.0.1:6379@16379 master,nofailover - 0 1 1 connected 0-2047\n" +
		"node2 11.0.0.2:6379@16379 myself,master,nofailover - 0 1 2 connected 2048-4095\n" +
		"node3 11.0.0.3:6379@16379 slave,node1 - 0 1 3 connected\n")

	nodes := ParseClusterNode(content)
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(nodes))
	}

	if nodes[0].Flags != TypeMaster {
		t.Fatalf("expected first node role %q, got %q", TypeMaster, nodes[0].Flags)
	}
	if nodes[1].Flags != TypeMaster {
		t.Fatalf("expected second node role %q, got %q", TypeMaster, nodes[1].Flags)
	}
	if nodes[2].Flags != TypeSlave {
		t.Fatalf("expected third node role %q, got %q", TypeSlave, nodes[2].Flags)
	}
}
