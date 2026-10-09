import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';

import styles from './index.module.css';

function StudyDisclaimer() {
  return (
    <div
      style={{
        background: '#fef3c7',
        color: '#78350f',
        textAlign: 'center',
        padding: '0.6rem 1rem',
        fontSize: '0.9rem',
        borderBottom: '1px solid #f59e0b',
      }}>
      ⚠️ <strong>Study project</strong> — an educational DDD exercise. Not a
      production system.
    </div>
  );
}

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero', styles.heroBanner)}>
      <StudyDisclaimer />
      <div className="container">
        <p className={styles.eyebrow}>
          warehouse-systems · WES tier · Core subdomain
        </p>
        <Heading as="h1" className={styles.heroTitle}>
          {siteConfig.title}
        </Heading>
        <p className={styles.heroSubtitle}>{siteConfig.tagline}</p>
        <p className={styles.heroLead}>
          Three local read models (site capability, site/SKU demand and
          published capacity plans) feed a fail-closed planning snapshot. An
          operator approves a transfer; the InterWarehouseTransfer saga then
          asks inventory-storage for a reservation, releases pick and dispatch
          work to the WES and follows the stock to the destination stow, all
          through a transactional outbox.
        </p>
        <div className={styles.buttons}>
          <Link
            className="button button--primary button--lg"
            to="/docs/overview/introduction">
            Read the docs
          </Link>
          <Link
            className="button button--secondary button--lg"
            to="/docs/api-reference/overview">
            API Reference
          </Link>
          <Link className="button button--secondary button--lg" to="/docs/adr/about">
            ADRs
          </Link>
        </div>
      </div>
    </header>
  );
}

export default function Home(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title={siteConfig.title}
      description="Documentation for the Network Inventory Planning bounded context: advisory transfer proposals, the fail-closed planning snapshot, the inter-warehouse transfer saga and its analytics read side.">
      <HomepageHeader />
      <main>
        <section className={styles.invariant}>
          <div className="container">
            <blockquote className={styles.invariantQuote}>
              Missing or stale facts never become zeros: a site without
              capability, demand and capacity facts is{' '}
              <strong>excluded, and an empty or stale snapshot refuses</strong>{' '}
              rather than recommending a transfer from partial state.
            </blockquote>
            <p className={styles.invariantCaption}>
              <Link to="/docs/ddd/aggregate-design-canvas">
                The InterWarehouseTransfer saga aggregate →
              </Link>
            </p>
          </div>
        </section>
      </main>
    </Layout>
  );
}
