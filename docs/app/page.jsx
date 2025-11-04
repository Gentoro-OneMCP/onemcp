"use client"

import { useEffect } from 'react'
import Image from 'next/image'

const heroHighlights = [
  'Expose every API operation through a single natural-language surface',
  'Cache execution plans for similar prompts and replay them instantly',
  'Pre-warm deployments so production runs without live inference',
]

const metrics = [
  { value: '90%', label: 'less inference spend with cached plans' },
  { value: '<120ms', label: 'plan replay latency in static mode' },
  { value: '1 tool', label: 'to expose your entire API through MCP' },
  { value: '24/7', label: 'deterministic execution with full observability' },
]

const pillars = [
  {
    eyebrow: 'Accuracy',
    title: 'Plans grounded in your handbook',
    description:
      'OneMCP reads your API specification, documentation, and policies so every plan stays aligned with the operations and parameters you already maintain.',
  },
  {
    eyebrow: 'Cost',
    title: 'Inference where it matters, cache everywhere else',
    description:
      'Generate an execution plan a single time, then replay it indefinitely. Similar prompts reuse cached logic without touching a model.',
  },
  {
    eyebrow: 'Performance',
    title: 'CPU-speed automation with deterministic outputs',
    description:
      'Deploy warm caches to keep production workloads fast, predictable, and dependable no matter the volume of requests.',
  },
]

const timelineSteps = [
  {
    title: 'Import the handbook',
    description: 'Ingest your API specification, docs, and authentication details to establish complete operational context.',
  },
  {
    title: 'Generate execution plans',
    description: 'OneMCP interprets incoming prompts, builds multi-step plans, and executes them safely against your API.',
  },
  {
    title: 'Reuse cached logic',
    description: 'Cached plans are retrieved for similar prompts, removing redundant reasoning and stabilising latency.',
  },
  {
    title: 'Deploy with a warm cache',
    description: 'Export the cache alongside the runtime so production runs without inference, with predictable cost and behaviour.',
  },
]

const runtimeModes = [
  {
    eyebrow: 'Recommended for production',
    title: 'Static mode',
    description:
      'Serve only prebuilt or cached execution plans. No runtime inference, fully deterministic behaviour, and effortless governance for regulated environments.',
    bullets: [
      'Zero model footprint once deployed',
      'Versioned plans you can audit or roll back',
      'Stable performance even under heavy load',
    ],
  },
  {
    eyebrow: 'Perfect for exploration',
    title: 'Dynamic mode',
    description:
      'Generate new plans on the fly when a prompt is unseen. Every plan is cached for reuse so experimentation turns into production-ready assets.',
    bullets: [
      'Natural-language prototyping with instant feedback',
      'Caches grow automatically as teams iterate',
      'One interface across development and production',
    ],
  },
]

const featureRows = [
  {
    title: 'Open source core',
    description: 'Ship the full MCP runtime, cache, and plan export pipeline under an open licence so your stack stays portable.',
  },
  {
    title: 'Governance ready',
    description: 'Plug in tracing, policy enforcement, and audit tooling. Enterprise extensions add observability, feedback loops, and optimization.',
  },
  {
    title: 'Built for every agent',
    description: 'Works with any MCP-capable agent or orchestrator, ensuring compatibility as new LLMs and frameworks arrive.',
  },
]

const marqueeItems = [
  { label: 'Anthropic', type: 'anthropic' },
  { label: 'OpenAI', type: 'image', src: '/logos/openai.svg' },
  { label: 'Gemini', type: 'image', src: '/logos/gemini.svg' },
  { label: 'Azure OpenAI', type: 'image', src: '/logos/azure-openai.svg' },
  { label: 'AWS Bedrock', type: 'image', src: '/logos/bedrock.svg' },
  { label: 'LangChain', type: 'image', src: '/logos/langchain-color.svg' },
  { label: 'Fireworks.ai', type: 'pill' },
  { label: 'MCP-native', type: 'pill' },
]

const socialLinks = [
  {
    label: 'GitHub',
    href: 'https://github.com/Gentoro-OneMCP/onemcp',
    icon: (
      <svg viewBox="0 0 24 24" width="20" height="20" fill="currentColor" aria-hidden="true">
        <path d="M12 .5a11.5 11.5 0 0 0-3.64 22.41c.58.11.8-.25.8-.56v-1.97c-3.26.71-3.95-1.57-3.95-1.57-.53-1.36-1.31-1.72-1.31-1.72-1.07-.74.08-.72.08-.72 1.18.08 1.8 1.21 1.8 1.21 1.05 1.81 2.75 1.29 3.42.99.11-.76.41-1.29.75-1.59-2.6-.3-5.34-1.31-5.34-5.82 0-1.29.46-2.33 1.21-3.15-.12-.3-.53-1.52.11-3.16 0 0 1-.32 3.3 1.2a11.4 11.4 0 0 1 6 0c2.3-1.52 3.3-1.2 3.3-1.2.64 1.64.23 2.86.11 3.16.75.82 1.21 1.86 1.21 3.15 0 4.52-2.75 5.52-5.36 5.81.42.36.8 1.07.8 2.17v3.22c0 .31.21.67.81.55A11.5 11.5 0 0 0 12 .5Z" />
      </svg>
    ),
  },
  {
    label: 'LinkedIn',
    href: 'https://www.linkedin.com/company/gentoro/',
    icon: (
      <svg viewBox="0 0 24 24" width="20" height="20" fill="currentColor" aria-hidden="true">
        <path d="M20.45 20.45h-3.55v-5.42c0-1.29-.02-2.95-1.8-2.95-1.8 0-2.07 1.4-2.07 2.85v5.52H9.48V9h3.41v1.56h.05c.47-.88 1.6-1.8 3.29-1.8 3.52 0 4.17 2.32 4.17 5.33v6.36ZM5.34 7.43a2.07 2.07 0 1 1 0-4.14 2.07 2.07 0 0 1 0 4.14Zm1.78 13.02H3.56V9h3.56v11.45Z" />
      </svg>
    ),
  },
  {
    label: 'Email',
    href: 'mailto:support@gentoro.com',
    icon: (
      <svg viewBox="0 0 24 24" width="20" height="20" fill="currentColor" aria-hidden="true">
        <path d="M20 4H4a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2V6a2 2 0 0 0-2-2Zm-.4 2-7.1 5.08L4.4 6ZM4 18V8l8.23 5.89L20 8v10Z" />
      </svg>
    ),
  },
]

function useInViewAnimation(selectors) {
  const selectorKey = selectors.join('|')
  useEffect(() => {
    if (typeof window === 'undefined') return
    const rootEl = document.documentElement
    rootEl.classList.add('js-enabled')
    const elements = Array.from(document.querySelectorAll(selectors.join(',')))
    if (!elements.length || !('IntersectionObserver' in window)) return

    const observer = new IntersectionObserver(
      entries => {
        entries.forEach(entry => {
          if (entry.isIntersecting) {
            entry.target.setAttribute('data-in-view', 'true')
            observer.unobserve(entry.target)
          }
        })
      },
      {
        threshold: 0.25,
        rootMargin: '0px 0px -10% 0px',
      },
    )

    const isInView = el => {
      const rect = el.getBoundingClientRect()
      const vh = window.innerHeight || document.documentElement.clientHeight
      return rect.top <= vh * 0.8 && rect.bottom >= 0
    }

    elements.forEach(el => {
      if (isInView(el)) {
        el.setAttribute('data-in-view', 'true')
      }
      observer.observe(el)
    })

    if (!rootEl.classList.contains('animations-ready')) {
      requestAnimationFrame(() => {
        rootEl.classList.add('animations-ready')
      })
    }

    return () => observer.disconnect()
  }, [selectorKey])
}

export default function HomePage() {
  useInViewAnimation(['.metrics-strip', '.timeline', '.mode-panels'])

  return (
    <div className="landing-page">
      <section className="hero">
        <div className="hero-backdrop" aria-hidden="true"></div>
        <div className="hero-shell">
          <div className="hero-content">
          <div className="hero-pill">Open-source OneMCP runtime</div>
            <h1 className="hero-title">Cache the intelligence. Serve the action.</h1>
            <p className="hero-subtitle">
              OneMCP turns natural-language prompts into cached execution plans so agents fulfil API requests instantly—with enterprise-grade accuracy, cost control, and performance.
            </p>
          <div className="hero-actions">
            <a href="/docs" className="btn-primary">Read the docs</a>
            <a href="https://github.com/Gentoro-OneMCP/onemcp" target="_blank" rel="noreferrer" className="btn-secondary">
              View on GitHub
            </a>
          </div>
          </div>
          <div className="hero-visual" aria-hidden="true">
            <div className="hero-glow"></div>
            <div className="hero-frame">
              <header className="hero-frame-header">
                <span className="hero-frame-badge">Execution plan</span>
                <span className="hero-frame-pill">Cached · Warm</span>
              </header>
              <ul className="hero-frame-list">
                <li>
                  <span className="hero-frame-step">1</span>
                  <div>
                    <strong>Lookup handbook</strong>
                    <p>Resolve the `createCustomer` operation and required authentication scope.</p>
                  </div>
                </li>
                <li>
                  <span className="hero-frame-step">2</span>
                  <div>
                    <strong>Assemble payload</strong>
                    <p>Jane Doe · abc@def.com · membership tier: VIP</p>
                  </div>
                </li>
                <li>
                  <span className="hero-frame-step">3</span>
                  <div>
                    <strong>Execute via MCP</strong>
                    <p>Replay plan · capture logs · store result for reuse.</p>
                  </div>
                </li>
              </ul>
            </div>
          </div>
        </div>
      </section>

      <section className="logo-strip brand-marquee" aria-label="Compatible providers and frameworks">
        <div className="logo-content">
          <h3 className="logo-title">Plays nicely with your AI stack</h3>
          <div className="logo-marquee">
            <div className="logo-track">
              {[0, 1].map(loopIndex => (
                <div
                  className="logo-sequence"
                  key={`sequence-${loopIndex}`}
                  aria-hidden={loopIndex === 1}
                >
                  {marqueeItems.map((item, index) => (
                    <div className="logo-item" key={`${loopIndex}-${index}`}>
                      {item.type === 'anthropic' && (
                        <svg viewBox="0 0 24 24" width="56" height="56" fill="currentColor" aria-hidden={loopIndex === 1}>
                          <path d="M13.827 3.52h3.603L24 20h-3.603l-6.57-16.48zm-7.258 0h3.767L16.906 20h-3.674l-1.343-3.461H5.017l-1.344 3.46H0L6.57 3.522zm4.132 9.959L8.453 7.687 6.205 13.48H10.7z" />
                        </svg>
                      )}
                      {item.type === 'image' && (
                        <Image src={item.src} alt={item.label} width={56} height={56} aria-hidden={loopIndex === 1} />
                      )}
                      {item.type === 'pill' && (
                        <span className="logo-pill" aria-hidden={loopIndex === 1}>{item.label}</span>
                      )}
                      {item.type !== 'pill' && <span className="logo-label">{item.label}</span>}
                    </div>
                  ))}
                </div>
              ))}
            </div>
          </div>
        </div>
      </section>

      <section className="intro-section">
        <div className="intro-container">
          <p className="intro-eyebrow">Why OneMCP</p>
          <h2 className="intro-heading">A compiled interface for every agent that touches your API.</h2>
          <p className="intro-body">
            Model Context Protocol solved connectivity. OneMCP solves the rest—accuracy, latency, and cost—by transforming prompts into cached execution plans. Agents get a natural-language surface; your systems get deterministic automation with observability, governance, and reuse built in.
          </p>
        </div>
      </section>

      <section className="pillar-section">
        <div className="pillar-grid">
          {pillars.map(pillar => (
            <article className="pillar-item" key={pillar.title}>
              <span className="pillar-eyebrow">{pillar.eyebrow}</span>
              <h3>{pillar.title}</h3>
              <p>{pillar.description}</p>
            </article>
          ))}
        </div>
      </section>

      <section className="timeline-section">
        <div className="section-heading">
          <h2 className="section-title">From handbook to production cache</h2>
          <p className="section-subtitle">Four steps turn your API into a reusable execution engine for every agent you ship.</p>
        </div>
        <div className="timeline">
          {timelineSteps.map(step => (
            <div className="timeline-item" key={step.title}>
              <div className="timeline-dot"></div>
              <div className="timeline-copy">
                <h3>{step.title}</h3>
                <p>{step.description}</p>
              </div>
            </div>
          ))}
        </div>
      </section>

      <section className="promise-section">
        <div className="promise-shell">
          <p className="promise-eyebrow">What cached execution delivers</p>
          <div className="promise-grid">
            {heroHighlights.map((item, index) => (
              <article className="promise-card" key={item}>
                <span className="promise-icon">{String(index + 1).padStart(2, '0')}</span>
                <p>{item}</p>
              </article>
            ))}
          </div>
        </div>
      </section>

      <section className="modes-section">
        <div className="section-heading">
          <h2 className="section-title">Optimize for your runtime</h2>
          <p className="section-subtitle">Switch between static and dynamic plans without changing how agents interact with your API.</p>
        </div>
        <div className="mode-panels">
          {runtimeModes.map(mode => (
            <article className="mode-panel" key={mode.title}>
              <div className="mode-gradient" aria-hidden="true"></div>
              <div className="mode-inner">
                <span className="mode-eyebrow">{mode.eyebrow}</span>
                <h3>{mode.title}</h3>
                <p>{mode.description}</p>
                <ul>
                  {mode.bullets.map(bullet => (
                    <li key={bullet}>{bullet}</li>
                  ))}
                </ul>
              </div>
            </article>
          ))}
        </div>
      </section>

      <section className="experience-section">
        <div className="experience-grid">
          <div className="experience-copy">
            <p className="experience-eyebrow">Example workflow</p>
            <h2>Prompt → plan → replay</h2>
            <p>
              “Create an account for Jane Doe with abc@def.com and set her up as a VIP member.” OneMCP interprets the intent, generates the correct API sequence, and stores that plan. The next time a similar request arrives, execution happens at cache speed—with full logging, policy enforcement, and deterministic output.
            </p>
            <ul>
              <li>Prompt interpretation aligned with your handbook</li>
              <li>Automatic plan storage and versioning</li>
              <li>Replay without inference or additional cost</li>
            </ul>
          </div>
          <div className="experience-visual" aria-hidden="true">
            <div className="experience-orbit">
              <div className="orbit-line"></div>
              <div className="orbit-node orbit-node--prompt">Prompt</div>
              <div className="orbit-node orbit-node--plan">Plan</div>
              <div className="orbit-node orbit-node--replay">Replay</div>
            </div>
          </div>
        </div>
      </section>

      <section className="feature-rows">
        {featureRows.map(row => (
          <article className="feature-row" key={row.title}>
            <div className="feature-row-icon" aria-hidden="true"></div>
            <div className="feature-row-copy">
              <h3>{row.title}</h3>
              <p>{row.description}</p>
            </div>
          </article>
        ))}
      </section>

      <section className="quote-section">
        <div className="quote-panel">
            <p className="quote-eyebrow">Engineered for growth</p>
          <blockquote>
            “APIs are finite and predictable. Once execution plans exist, OneMCP delivers the speed of compiled code with the flexibility of natural language.”
          </blockquote>
          <p className="quote-footer">Smart caching and pre-warmed deployment are in active development—plan storage and reuse ship today.</p>
        </div>
      </section>

      <section className="metrics-strip">
        <div className="metrics-grid">
          {metrics.map(metric => (
            <div className="metric" key={metric.label}>
              <span className="metric-value">{metric.value}</span>
              <span className="metric-label">{metric.label}</span>
            </div>
          ))}
        </div>
      </section>

      <section className="cta-section gradient-cta">
        <div className="cta-content">
          <h2>Start building with OneMCP today</h2>
          <p>Read the handbook guide or clone the repo to create your first cached execution plan.</p>
          <a href="/docs" className="btn-primary btn-large">Launch the docs</a>
        </div>
      </section>

      <footer className="landing-footer" aria-label="OneMCP footer">
        <div className="footer-inner">
          <div className="footer-about">
            <h3>OneMCP</h3>
            <p>
              Open-source runtime that turns your API handbook into reusable execution plans—fast, accurate, and production ready for every agent.
            </p>
            <div className="footer-social" aria-label="Social links">
              {socialLinks.map(link => (
                <a key={link.label} href={link.href} target="_blank" rel="noreferrer" aria-label={link.label}>
                  <span className="sr-only">{link.label}</span>
                  {link.icon}
                </a>
              ))}
            </div>
          </div>

          <div className="footer-links">
            <div>
              <h4>Product</h4>
              <ul>
                <li><a href="/docs">Documentation</a></li>
                <li><a href="/docs/guides/ingest-foundation">Handbook ingest</a></li>
                <li><a href="https://github.com/Gentoro-OneMCP/onemcp" target="_blank" rel="noreferrer">Source code</a></li>
              </ul>
            </div>
            {/* <div>
              <h4>Resources</h4>
              <ul>
                <li><a href="/docs/get-started">Getting started</a></li>
                <li><a href="/docs/architecture">Architecture</a></li>
                <li><a href="/docs/changelog">Changelog</a></li>
              </ul>
            </div> */}
            <div>
              <h4>Company</h4>
              <ul>
                <li><a href="https://gentoro.com" target="_blank" rel="noreferrer">Gentoro</a></li>
                <li><a href="https://www.gentoro.com/contact">Contact us</a></li>
              </ul>
            </div>
          </div>
        </div>
      </footer>
    </div>
  )
}
