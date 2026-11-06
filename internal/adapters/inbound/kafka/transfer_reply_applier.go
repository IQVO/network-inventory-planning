package kafka

import (
	"context"

	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
)

// TransferReplyApplier is the application surface the reply consumer
// drives. implemented by ReplyUseCases (a pair of the two use cases).
type TransferReplyApplier interface {
	ApplyAllocation(ctx context.Context, in usecases.ApplyAllocationInput) error
	ApplyRejection(ctx context.Context, in usecases.ApplyRejectionInput) error
	// The two destination facts (ADR 0005), same topic, same consumer.
	ApplyReceiptStaged(ctx context.Context, in usecases.ApplyReceiptStagedInput) error
	ApplyStowed(ctx context.Context, in usecases.ApplyStowInput) error
	// The release fact (ADR 0022), same topic, same consumer.
	ApplyReleased(ctx context.Context, in usecases.ApplyReleasedInput) error
}

// ReplyUseCases pairs the reply use cases into a TransferReplyApplier.
type ReplyUseCases struct {
	Allocate usecases.ApplyTransferAllocation
	Reject   usecases.ApplyTransferRejection
	Staged   usecases.ApplyTransferReceiptStaged
	Stow     usecases.ApplyTransferStow
	Released usecases.ApplyTransferAllocationReleased
}

// ApplyAllocation implements TransferReplyApplier.
func (r ReplyUseCases) ApplyAllocation(ctx context.Context, in usecases.ApplyAllocationInput) error {
	return r.Allocate.Execute(ctx, in)
}

// ApplyRejection implements TransferReplyApplier.
func (r ReplyUseCases) ApplyRejection(ctx context.Context, in usecases.ApplyRejectionInput) error {
	return r.Reject.Execute(ctx, in)
}

// ApplyReceiptStaged implements TransferReplyApplier.
func (r ReplyUseCases) ApplyReceiptStaged(ctx context.Context, in usecases.ApplyReceiptStagedInput) error {
	return r.Staged.Execute(ctx, in)
}

// ApplyStowed implements TransferReplyApplier.
func (r ReplyUseCases) ApplyStowed(ctx context.Context, in usecases.ApplyStowInput) error {
	return r.Stow.Execute(ctx, in)
}

// ApplyReleased implements TransferReplyApplier.
func (r ReplyUseCases) ApplyReleased(ctx context.Context, in usecases.ApplyReleasedInput) error {
	return r.Released.Execute(ctx, in)
}
