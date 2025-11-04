import Image from 'next/image'

export default function HomePage() {
  return (
    <div className="landing-page">
      {/* Hero Section */}
      <section className="hero">
        <div className="hero-content">
          <div className="hero-pill">Ship production‑ready MCP tools with no custom code</div>
          <h1 className="hero-title">Secure AI Integration for the Enterprise</h1>
          <p className="hero-subtitle">
            Confidently execute AI-native tools with built-in security, traceability, and 
            comprehensive support for the Model Context Protocol
          </p>
          <div className="hero-actions">
            <a href="/docs" className="btn-primary">Get Started</a>
            <a href="https://github.com/gentoro-GT/onemcp" target="_blank" rel="noreferrer" className="btn-secondary">
              GitHub
            </a>
          </div>
        </div>
      </section>

      {/* Logo strip */}
      <section className="logo-strip">
        <div className="logo-content">
          <h3 className="logo-title">Built to Work Across Your AI Stack</h3>
          <ul className="logo-row" aria-label="Supported providers">
            <li>
              <svg
                viewBox="0 0 24 24"
                width="24"
                height="24"
                fill="currentColor"
                fillRule="evenodd"
                style={{ flex: 'none', lineHeight: 1 }}
                xmlns="http://www.w3.org/2000/svg"
                aria-hidden="true"
              >
                <title>Anthropic</title>
                <path d="M13.827 3.52h3.603L24 20h-3.603l-6.57-16.48zm-7.258 0h3.767L16.906 20h-3.674l-1.343-3.461H5.017l-1.344 3.46H0L6.57 3.522zm4.132 9.959L8.453 7.687 6.205 13.48H10.7z" />
              </svg>
            </li>
            <li>
              <svg 
                xmlns="http://www.w3.org/2000/svg" 
                width="24" 
                height="24" 
                preserveAspectRatio="xMidYMid" 
                viewBox="0 0 256 256"
                fill="currentColor"
              >
                <path d="M239.184 106.203a64.716 64.716 0 0 0-5.576-53.103C219.452 28.459 191 15.784 163.213 21.74A65.586 65.586 0 0 0 52.096 45.22a64.716 64.716 0 0 0-43.23 31.36c-14.31 24.602-11.061 55.634 8.033 76.74a64.665 64.665 0 0 0 5.525 53.102c14.174 24.65 42.644 37.324 70.446 31.36a64.72 64.72 0 0 0 48.754 21.744c28.481.025 53.714-18.361 62.414-45.481a64.767 64.767 0 0 0 43.229-31.36c14.137-24.558 10.875-55.423-8.083-76.483Zm-97.56 136.338a48.397 48.397 0 0 1-31.105-11.255l1.535-.87 51.67-29.825a8.595 8.595 0 0 0 4.247-7.367v-72.85l21.845 12.636c.218.111.37.32.409.563v60.367c-.056 26.818-21.783 48.545-48.601 48.601Zm-104.466-44.61a48.345 48.345 0 0 1-5.781-32.589l1.534.921 51.722 29.826a8.339 8.339 0 0 0 8.441 0l63.181-36.425v25.221a.87.87 0 0 1-.358.665l-52.335 30.184c-23.257 13.398-52.97 5.431-66.404-17.803ZM23.549 85.38a48.499 48.499 0 0 1 25.58-21.333v61.39a8.288 8.288 0 0 0 4.195 7.316l62.874 36.272-21.845 12.636a.819.819 0 0 1-.767 0L41.353 151.53c-23.211-13.454-31.171-43.144-17.804-66.405v.256Zm179.466 41.695-63.08-36.63L161.73 77.86a.819.819 0 0 1 .768 0l52.233 30.184a48.6 48.6 0 0 1-7.316 87.635v-61.391a8.544 8.544 0 0 0-4.4-7.213Zm21.742-32.69-1.535-.922-51.619-30.081a8.39 8.39 0 0 0-8.492 0L99.98 99.808V74.587a.716.716 0 0 1 .307-.665l52.233-30.133a48.652 48.652 0 0 1 72.236 50.391v.205ZM88.061 139.097l-21.845-12.585a.87.87 0 0 1-.41-.614V65.685a48.652 48.652 0 0 1 79.757-37.346l-1.535.87-51.67 29.825a8.595 8.595 0 0 0-4.246 7.367l-.051 72.697Zm11.868-25.58 28.138-16.217 28.188 16.218v32.434l-28.086 16.218-28.188-16.218-.052-32.434Z"/>
              </svg>
            </li>
            <li><svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" preserveAspectRatio="xMidYMid" viewBox="0 0 256 257"><path fill="#D97757" d="m50.228 170.321 50.357-28.257.843-2.463-.843-1.361h-2.462l-8.426-.518-28.775-.778-24.952-1.037-24.175-1.296-6.092-1.297L0 125.796l.583-3.759 5.12-3.434 7.324.648 16.202 1.101 24.304 1.685 17.629 1.037 26.118 2.722h4.148l.583-1.685-1.426-1.037-1.101-1.037-25.147-17.045-27.22-18.017-14.258-10.37-7.713-5.25-3.888-4.925-1.685-10.758 7-7.713 9.397.649 2.398.648 9.527 7.323 20.35 15.75L94.817 91.9l3.889 3.24 1.555-1.102.195-.777-1.75-2.917-14.453-26.118-15.425-26.572-6.87-11.018-1.814-6.61c-.648-2.723-1.102-4.991-1.102-7.778l7.972-10.823L71.42 0 82.05 1.426l4.472 3.888 6.61 15.101 10.694 23.786 16.591 32.34 4.861 9.592 2.592 8.879.973 2.722h1.685v-1.556l1.36-18.211 2.528-22.36 2.463-28.776.843-8.1 4.018-9.722 7.971-5.25 6.222 2.981 5.12 7.324-.713 4.73-3.046 19.768-5.962 30.98-3.889 20.739h2.268l2.593-2.593 10.499-13.934 17.628-22.036 7.778-8.749 9.073-9.657 5.833-4.601h11.018l8.1 12.055-3.628 12.443-11.342 14.388-9.398 12.184-13.48 18.147-8.426 14.518.778 1.166 2.01-.194 30.46-6.481 16.462-2.982 19.637-3.37 8.88 4.148.971 4.213-3.5 8.62-20.998 5.184-24.628 4.926-36.682 8.685-.454.324.519.648 16.526 1.555 7.065.389h17.304l32.21 2.398 8.426 5.574 5.055 6.805-.843 5.184-12.962 6.611-17.498-4.148-40.83-9.721-14-3.5h-1.944v1.167l11.666 11.406 21.387 19.314 26.767 24.887 1.36 6.157-3.434 4.86-3.63-.518-23.526-17.693-9.073-7.972-20.545-17.304h-1.36v1.814l4.73 6.935 25.017 37.59 1.296 11.536-1.814 3.76-6.481 2.268-7.13-1.297-14.647-20.544-15.1-23.138-12.185-20.739-1.49.843-7.194 77.448-3.37 3.953-7.778 2.981-6.48-4.925-3.436-7.972 3.435-15.749 4.148-20.544 3.37-16.333 3.046-20.285 1.815-6.74-.13-.454-1.49.194-15.295 20.999-23.267 31.433-18.406 19.702-4.407 1.75-7.648-3.954.713-7.064 4.277-6.286 25.47-32.405 15.36-20.092 9.917-11.6-.065-1.686h-.583L44.07 198.125l-12.055 1.555-5.185-4.86.648-7.972 2.463-2.593 20.35-13.999-.064.065Z"/></svg></li>
            <li><Image src="/logos/langchain-color.svg" alt="LangChain" width={28} height={28} priority /></li>
            <li><Image src="/logos/gemini.svg" alt="LangChain" width={28} height={28} priority /></li>
          </ul>
        </div>
      </section>

      {/* Demo Section (CLI style) */}
      <section className="demo-section">
        <h2 className="section-title">See Gentoro in Action</h2>
        <div className="demo-window">
          <div className="demo-header">
            <span className="demo-dot"></span>
            <span className="demo-dot"></span>
            <span className="demo-dot"></span>
          </div>
          <div className="demo-content">
            <p className="demo-text">$ onemcp run --tools openapi.yaml</p>
            <p className="demo-text">✓ Indexed 32 tools from OpenAPI spec</p>
            <p className="demo-text">✓ Knowledge base ready (457 documents)</p>
            <p className="demo-text">→ Agent running on http://localhost:3000</p>
          </div>
        </div>
      </section>

      {/* Features Section */}
      <section className="features-section">
        <h2 className="section-title">Easily Generate, Manage, and Secure MCP Tools</h2>
        <div className="features-grid">
          <div className="feature-card">
            <div className="feature-icon" aria-hidden="true">
              <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 448 512" role="img" focusable="false">
                <path d="M80 104a24 24 0 1 0 0-48 24 24 0 1 0 0 48zm80-24c0 32.8-19.7 61-48 73.3l0 87.8c18.8-10.9 40.7-17.1 64-17.1l96 0c35.3 0 64-28.7 64-64l0-6.7C307.7 141 288 112.8 288 80c0-44.2 35.8-80 80-80s80 35.8 80 80c0 32.8-19.7 61-48 73.3l0 6.7c0 70.7-57.3 128-128 128l-96 0c-35.3 0-64 28.7-64 64l0 6.7c28.3 12.3 48 40.5 48 73.3c0 44.2-35.8 80-80 80s-80-35.8-80-80c0-32.8 19.7-61 48-73.3l0-6.7 0-198.7C19.7 141 0 112.8 0 80C0 35.8 35.8 0 80 0s80 35.8 80 80zm232 0a24 24 0 1 0 -48 0 24 24 0 1 0 48 0zM80 456a24 24 0 1 0 0-48 24 24 0 1 0 0 48z" fill="currentColor"/>
              </svg>
            </div>
            <h3>Generate</h3>
            <p>Dynamically generate safe, executable code from natural language instructions with built-in validation</p>
          </div>
          <div className="feature-card">
            <div className="feature-icon" aria-hidden="true">
              <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 640 512" role="img" focusable="false">
                <path d="M308.5 135.3c7.1-6.3 9.9-16.2 6.2-25c-2.3-5.3-4.8-10.5-7.6-15.5L304 89.4c-3-5-6.3-9.9-9.8-14.6c-5.7-7.6-15.7-10.1-24.7-7.1l-28.2 9.3c-10.7-8.8-23-16-36.2-20.9L199 27.1c-1.9-9.3-9.1-16.7-18.5-17.8C173.9 8.4 167.2 8 160.4 8l-.7 0c-6.8 0-13.5 .4-20.1 1.2c-9.4 1.1-16.6 8.6-18.5 17.8L115 56.1c-13.3 5-25.5 12.1-36.2 20.9L50.5 67.8c-9-3-19-.5-24.7 7.1c-3.5 4.7-6.8 9.6-9.9 14.6l-3 5.3c-2.8 5-5.3 10.2-7.6 15.6c-3.7 8.7-.9 18.6 6.2 25l22.2 19.8C32.6 161.9 32 168.9 32 176s.6 14.1 1.7 20.9L11.5 216.7c-7.1 6.3-9.9 16.2-6.2 25c2.3 5.3 4.8 10.5 7.6 15.6l3 5.2c3 5.1 6.3 9.9 9.9 14.6c5.7 7.6 15.7 10.1 24.7 7.1l28.2-9.3c10.7 8.8 23 16 36.2 20.9l6.1 29.1c1.9 9.3 9.1 16.7 18.5 17.8c6.7 .8 13.5 1.2 20.4 1.2s13.7-.4 20.4-1.2c9.4-1.1 16.6-8.6 18.5-17.8l6.1-29.1c13.3-5 25.5-12.1 36.2-20.9l28.2 9.3c9 3 19 .5 24.7-7.1c3.5-4.7 6.8-9.5 9.8-14.6l3.1-5.4c2.8-5 5.3-10.2 7.6-15.5c3.7-8.7 .9-18.6-6.2-25l-22.2-19.8c1.1-6.8 1.7-13.8 1.7-20.9s-.6-14.1-1.7-20.9l22.2-19.8zM112 176a48 48 0 1 1 96 0 48 48 0 1 1 -96 0zM504.7 500.5c6.3 7.1 16.2 9.9 25 6.2c5.3-2.3 10.5-4.8 15.5-7.6l5.4-3.1c5-3 9.9-6.3 14.6-9.8c7.6-5.7 10.1-15.7 7.1-24.7l-9.3-28.2c8.8-10.7 16-23 20.9-36.2l29.1-6.1c9.3-1.9 16.7-9.1 17.8-18.5c.8-6.7 1.2-13.5 1.2-20.4s-.4-13.7-1.2-20.4c-1.1-9.4-8.6-16.6-17.8-18.5L583.9 307c-5-13.3-12.1-25.5-20.9-36.2l9.3-28.2c3-9 .5-19-7.1-24.7c-4.7-3.5-9.6-6.8-14.6-9.9l-5.3-3c-5-2.8-10.2-5.3-15.6-7.6c-8.7-3.7-18.6-.9-25 6.2l-19.8 22.2c-6.8-1.1-13.8-1.7-20.9-1.7s-14.1 .6-20.9 1.7l-19.8-22.2c-6.3-7.1-16.2-9.9-25-6.2c-5.3 2.3-10.5 4.8-15.6 7.6l-5.2 3c-5.1 3-9.9 6.3-14.6 9.9c-7.6 5.7-10.1 15.7-7.1 24.7l9.3 28.2c-8.8 10.7-16 23-20.9 36.2L315.1 313c-9.3 1.9-16.7 9.1-17.8 18.5c-.8 6.7-1.2 13.5-1.2 20.4s.4 13.7 1.2 20.4c1.1 9.4 8.6 16.6 17.8 18.5l29.1 6.1c5 13.3 12.1 25.5 20.9 36.2l-9.3 28.2c-3 9-.5 19 7.1 24.7c4.7 3.5 9.5 6.8 14.6 9.8l5.4 3.1c5 2.8 10.2 5.3 15.5 7.6c8.7 3.7 18.6 .9 25-6.2l19.8-22.2c6.8 1.1 13.8 1.7 20.9 1.7s14.1-.6 20.9-1.7l19.8 22.2zM464 304a48 48 0 1 1 0 96 48 48 0 1 1 0-96z" fill="currentColor"/>
              </svg>
            </div>
            <h3>Manage</h3>
            <p>Centralize tool definitions, manage versioning, and maintain consistent behavior across deployments</p>
          </div>
          <div className="feature-card">
            <div className="feature-icon" aria-hidden="true">
              <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512" role="img" focusable="false">
                <path d="M256 0c4.6 0 9.2 1 13.4 2.9L457.7 82.8c22 9.3 38.4 31 38.3 57.2c-.5 99.2-41.3 280.7-213.6 363.2c-16.7 8-36.1 8-52.8 0C57.3 420.7 16.5 239.2 16 140c-.1-26.2 16.3-47.9 38.3-57.2L242.7 2.9C246.8 1 251.4 0 256 0zm0 66.8l0 378.1C394 378 431.1 230.1 432 141.4L256 66.8s0 0 0 0z" fill="currentColor"/>
              </svg>
            </div>
            <h3>Secure</h3>
            <p>Enterprise-grade security with built-in guardrails, audit logs, and permission controls</p>
          </div>
        </div>
      </section>

      {/* Use Cases Section */}
      <section className="use-cases-section">
        <h2 className="section-title">Putting MCP Tools to Work</h2>
        <div className="work-grid">
          <article className="video-card">
            <div className="video-embed">
              <iframe
                src="https://www.youtube.com/embed/809s7xEJlSw"
                title="How to Automate Production Support With Gentoro"
                allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
                allowFullScreen
              ></iframe>
            </div>
            <h3>How to Automate Production Support With Gentoro</h3>
          </article>
          <article className="video-card">
            <div className="video-embed">
              <iframe
                src="https://www.youtube.com/embed/66sfR_GJ_zM"
                title="How to Generate MCP Tools From Any OpenAPI Spec"
                allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
                allowFullScreen
              ></iframe>
            </div>
            <h3>How to Generate MCP Tools From Any OpenAPI Spec</h3>
          </article>
        </div>
      </section>

      {/* Dark product panel */}
      <section className="panel-dark">
        <div className="panel-inner">
          <div className="panel-media" aria-hidden="true"></div>
          <div className="panel-copy">
            <h2>Agentic Automation Starts Here</h2>
            <ul className="benefits-list">
              <li>No coding required</li>
              <li>Works with any agent framework</li>
              <li>On-prem, secure, production ready</li>
              <li>No vendor lock-in, built for change</li>
            </ul>
            <a href="/docs" className="btn-primary">Try It Free</a>
          </div>
        </div>
      </section>

      {/* Why choose section */}
      <section className="why-section">
        <div className="why-container">
          <h2 className="section-title">Why Teams Choose Gentoro</h2>
          <div className="why-layout">
            <div className="why-left">
              <p className="why-lede">Powered by GenAI. MCP-native. Secure by design. Ready for production.</p>
              <p className="why-lede">Gentoro was built from the ground up to support enterprise-grade deployment and deliver immediate ROI.</p>

              <div className="why-item">
                <span className="check-circle" aria-hidden="true">✓</span>
                <div>
                  <h3>Faster Time to Impact</h3>
                  <p>Eliminate integration bottlenecks and ship AI‑powered workflows in record time. Build and deploy MCP Tools using familiar specs and zero code.</p>
                </div>
              </div>

              <div className="why-item">
                <span className="check-circle" aria-hidden="true">✓</span>
                <div>
                  <h3>Model & Framework Flexibility</h3>
                  <p>Avoid vendor lock‑in with confidence. Works across LLMs and agent orchestration frameworks while adapting as new technologies arrive.</p>
                </div>
              </div>

              <div className="why-item">
                <span className="check-circle" aria-hidden="true">✓</span>
                <div>
                  <h3>Operational Confidence & Governance at Scale</h3>
                  <p>Built‑in security, policy enforcement, auditability, and runtime services for monitoring and optimization in production.</p>
                </div>
              </div>

              <a href="/docs" className="btn-primary why-cta">Try for Free</a>
            </div>

            <div className="why-right">
              <article className="testimonial-card">
                <p className="quote">“Enterprises are standardizing on MCP and expect their technology partners to follow suit. Gentoro can help you fast‑track your MCP roadmap.”</p>
                <div className="testimonial-footer">
                  <img className="avatar" src="/images/NicholasGrabowskiPP.png" alt="Nicholas Grabowski" width="36" height="36" />
                  <div>
                    <div className="person">—Nicholas Grabowski</div>
                    <div className="role">CPO, Digital.ai</div>
                  </div>
                </div>
              </article>

              <article className="testimonial-card">
                <p className="quote">“We are adapting AI to create dynamic automation workflows. Time is of the essence and Gentoro gives us the fastest path from idea to value realization.”</p>
                <div className="testimonial-footer">
                  <img className="avatar" src="/images/SajitMenonPP.png" alt="Sajit Menon" width="36" height="36" />
                  <div>
                    <div className="person">—Sajit Menon</div>
                    <div className="role">Technology Strategy Consultant, CPG</div>
                  </div>
                </div>
              </article>

              <article className="testimonial-card">
                <p className="quote">“Thanks to Gentoro, we’re able to standardize on MCP and automate not just critical processes, but also how they interoperate with custom and packaged applications.”</p>
                <div className="testimonial-footer">
                  <img className="avatar" src="/images/DavidGleasonPP.png" alt="David Gleason" width="36" height="36" />
                  <div>
                    <div className="person">—David Gleason</div>
                    <div className="role">Chief AI Officer, All In On Data</div>
                  </div>
                </div>
              </article>
            </div>
          </div>
        </div>
      </section>

      {/* CTA Section */}
      <section className="cta-section gradient-cta">
        <div className="cta-content">
          <h2>Customized Plans for Real Enterprise Needs</h2>
          <p>We’ll help your team evaluate options and choose the right pricing model.</p>
          <a href="/docs" className="btn-primary btn-large">Get in Touch</a>
        </div>
      </section>
    </div>
  );
}
