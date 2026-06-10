package domain

// AggregateRequestStatus derives a request's business status from the statuses of
// its deliveries. With multi-channel fan-out a request may be partially delivered.
//
// Rules:
//   - any delivery still in progress      -> PENDING
//   - all terminal and all SENT           -> SENT
//   - all terminal and all CANCELLED      -> CANCELLED
//   - all terminal, none SENT, some FAILED-> FAILED
//   - all terminal with a mix incl. SENT  -> PARTIAL
func AggregateRequestStatus(deliveries []DeliveryStatus) RequestStatus {
	if len(deliveries) == 0 {
		return RequestPending
	}

	var sent, failed, cancelled int
	for _, s := range deliveries {
		if !s.IsTerminal() {
			return RequestPending
		}
		switch s {
		case DeliverySent:
			sent++
		case DeliveryFailed:
			failed++
		case DeliveryCancelled:
			cancelled++
		}
	}

	total := len(deliveries)
	switch {
	case sent == total:
		return RequestSent
	case cancelled == total:
		return RequestCancelled
	case sent == 0 && failed > 0:
		return RequestFailed
	default:
		return RequestPartial
	}
}
