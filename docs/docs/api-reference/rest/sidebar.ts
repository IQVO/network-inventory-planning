import type { SidebarsConfig } from "@docusaurus/plugin-content-docs";

const sidebar: SidebarsConfig = {
  apisidebar: [
    {
      type: "doc",
      id: "api-reference/rest/network-inventory-planning-api",
    },
    {
      type: "category",
      label: "health",
      link: {
        type: "doc",
        id: "api-reference/rest/health",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/get-health",
          label: "Liveness probe.",
          className: "api-method get",
        },
      ],
    },
    {
      type: "category",
      label: "planning",
      link: {
        type: "doc",
        id: "api-reference/rest/planning",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/generate-transfer-proposals",
          label: "Generate advisory transfer proposals from an explicit snapshot.",
          className: "api-method post",
        },
        {
          type: "doc",
          id: "api-reference/rest/simulate-transfer-options",
          label: "Simulate advisory transfer options from local read models.",
          className: "api-method get",
        },
      ],
    },
    {
      type: "category",
      label: "transfers",
      link: {
        type: "doc",
        id: "api-reference/rest/transfers",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/approve-transfer",
          label: "Approve a transfer proposal and start the allocation saga.",
          className: "api-method post",
        },
        {
          type: "doc",
          id: "api-reference/rest/list-transfers",
          label: "List transfers, newest first.",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest/get-transfer",
          label: "Get one transfer with its audit trail.",
          className: "api-method get",
        },
      ],
    },
    {
      type: "category",
      label: "rebalance",
      link: {
        type: "doc",
        id: "api-reference/rest/rebalance",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/list-rebalance-runs",
          label: "List scheduled rebalance runs, newest first.",
          className: "api-method get",
        },
      ],
    },
    {
      type: "category",
      label: "reports",
      link: {
        type: "doc",
        id: "api-reference/rest/reports",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/get-transfer-funnel-report",
          label: "How many distinct transfers reached each saga state, per day",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest/get-state-dwell-report",
          label: "Saga age percentiles at the moment of leaving each state, per day",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest/get-stuck-transfers-report",
          label: "TransferStuckDetected occurrences per state per day, plus the latest ones",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest/get-rebalance-runs-report",
          label: "Scheduled rebalance runs, proposals against rejected, per day",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest/get-reports-freshness",
          label: "How far the analytics projection is behind",
          className: "api-method get",
        },
      ],
    },
  ],
};

export default sidebar.apisidebar;
