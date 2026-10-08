import { screen, within } from "@testing-library/react";
import type { UserEvent } from "@testing-library/user-event";

export interface ApprovalInput {
  origin?: string;
  destination?: string;
  sku?: string;
  quantity?: string;
  policyVersion?: string;
  reason?: string;
}

/** Fills the operator-driven Approve form (everything the operator must supply). */
export async function fillApproval(user: UserEvent, input: ApprovalInput = {}): Promise<HTMLElement> {
  const form = await screen.findByRole("form", { name: "Approve transfer" });
  const f = within(form);
  await user.selectOptions(f.getByLabelText(/^Origin site/), input.origin ?? "DC-EAST");
  await user.selectOptions(f.getByLabelText(/^Destination site/), input.destination ?? "DC-WEST");
  await user.type(f.getByLabelText(/^SKU/), input.sku ?? "SKU-RED-42");
  await user.type(f.getByLabelText(/^Quantity/), input.quantity ?? "120");
  await user.type(f.getByLabelText(/^Policy version/), input.policyVersion ?? "v7");
  if (input.reason) await user.type(f.getByLabelText("Operator reason"), input.reason);
  return form;
}
