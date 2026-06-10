package domain

import "testing"

func TestChannelValid(t *testing.T) {
	if !ChannelSMS.Valid() || !ChannelEmail.Valid() || !ChannelPush.Valid() {
		t.Fatal("valid channels")
	}
	if Channel("fax").Valid() {
		t.Fatal("fax invalid")
	}
}

func TestPriorityValid(t *testing.T) {
	if !PriorityHigh.Valid() || !PriorityNormal.Valid() || !PriorityLow.Valid() {
		t.Fatal("valid priorities")
	}
	if Priority("urgent").Valid() {
		t.Fatal("urgent invalid")
	}
}

func TestAllChannelsAndPriorities(t *testing.T) {
	if len(AllChannels()) != 3 || len(AllPriorities()) != 3 {
		t.Fatal("expected 3 each")
	}
}

func TestDeliveryStatusTerminal(t *testing.T) {
	if !DeliverySent.IsTerminal() {
		t.Fatal()
	}
	if DeliveryPending.IsTerminal() {
		t.Fatal()
	}
}
