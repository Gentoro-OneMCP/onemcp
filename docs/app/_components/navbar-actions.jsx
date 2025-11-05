"use client";

import { useEffect, useState } from "react";

const REPO_SLUG = "Gentoro-OneMCP/onemcp";
const REPO_URL = `https://github.com/${REPO_SLUG}`;

const numberFormatter = new Intl.NumberFormat("en-US", {
  notation: "compact",
  maximumFractionDigits: 1,
});

function formatStars(count) {
  if (count == null) return "--";
  return numberFormatter.format(count);
}

export default function NavbarActions() {
  const [stars, setStars] = useState(null);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        const response = await fetch(`https://api.github.com/repos/${REPO_SLUG}`);
        if (!response.ok) {
          throw new Error("Failed to load repo metadata");
        }
        const data = await response.json();
        if (!cancelled) {
          setStars(typeof data?.stargazers_count === "number" ? data.stargazers_count : null);
        }
      } catch (error) {
        if (!cancelled) {
          setStars(null);
        }
      }
    }

    load();

    return () => {
      cancelled = true;
    };
  }, []);

  const display = formatStars(stars);

  return (
    <div className="navbar-actions" role="group" aria-label="GitHub actions">
      <a
        className="navbar-actions__stars"
        href={REPO_URL}
        target="_blank"
        rel="noreferrer"
        aria-label={`Star OneMCP on GitHub (${display} stars)`}
      >
        <svg viewBox="0 0 16 16" aria-hidden="true" focusable="false">
          <path
            fill="none"
            stroke="currentColor"
            strokeWidth="1.4"
            strokeLinejoin="round"
            d="m8 1.5 1.78 3.6 3.97.58-2.87 2.8.68 4L8 11.13l-3.56 1.87.68-4L2.25 5.68l3.97-.58L8 1.5Z"
          />
        </svg>
        <span aria-hidden="true">{display}</span>
        <span className="sr-only">{display} GitHub stars</span>
      </a>
    </div>
  );
}

