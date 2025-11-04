# OneMCP Documentation

This directory contains the documentation site for OneMCP, built with [Nextra 4](https://nextra.site/) on Next.js App Router.

## Prerequisites

- Node.js 18+ (recommended: use the version in `.nvmrc` if present)
- pnpm (install via `npm install -g pnpm`)

## Quick Start

Install dependencies:

```bash
pnpm install
```

Run the dev server:

```bash
pnpm run dev
```

Open [http://localhost:3000](http://localhost:3000) in your browser. The site auto-reloads on file changes.

## Project Structure

```
docs/
├── app/
│   ├── _meta.jsx          # Top-level navigation (Docs, Blog, Community)
│   ├── layout.jsx         # Site layout and theme configuration
│   └── docs/
│       ├── _meta.jsx      # Docs section ordering
│       ├── page.mdx       # Introduction page
│       ├── getting-started/
│       ├── concepts/
│       ├── guides/
│       └── reference/
├── public/                # Static assets (images, etc.)
├── next.config.mjs        # Next.js + Nextra configuration
└── package.json
```

## Adding Content

### Create a New Page

Add a `page.mdx` file in the appropriate folder:

```bash
docs/app/docs/guides/my-new-guide/page.mdx
```

Include front matter:

```mdx
---
description: A short description for SEO and navigation.
---

# My New Guide

Your content here...
```

### Update Sidebar Navigation

Edit the `_meta.jsx` file in the parent directory to control ordering and labels:

```js
// docs/app/docs/guides/_meta.jsx
export default {
  index: "Guides",
  "my-new-guide": "My New Guide",
  // ...
};
```

## Building for Production

```bash
pnpm run build
```

Output is in `.next/`. Serve locally:

```bash
pnpm run start
```

## Configuration

### Analytics Setup

The documentation site uses Google Tag Manager (GTM) for analytics tracking.

#### Local Development

Create a `.env.local` file in the `docs/` directory:

```bash
NEXT_PUBLIC_GTM_ID=GTM-XXXXXXX
```

Restart the dev server after adding the variable.

#### Production Deployment

Configure the GTM container ID in GitHub:

1. Go to **Settings** → **Environments** → **production** → **Variables**
2. Add environment variable:
   - Name: `NEXT_PUBLIC_GTM_ID`
   - Value: Your GTM container ID (e.g., `GTM-M5H432R3`)

The workflow will automatically inject this during the build step.

## Deployment

### Automated S3 Deployment

The documentation is deployed to S3 via a manually-triggered GitHub Actions workflow.

#### Setup Requirements

1. **AWS Secrets** (configure in GitHub repository settings):
   - `AWS_DEPLOY_ROLE_ARN` - IAM role ARN with S3 write permissions
   - `AWS_REGION` - AWS region (e.g., `us-east-1`)
   - `DOCS_S3_BUCKET` - S3 bucket name for docs
   - `CLOUDFRONT_DISTRIBUTION_ID` - (Optional) CloudFront distribution ID for cache invalidation

2. **Analytics** (configure in GitHub Environments):
   - `NEXT_PUBLIC_GTM_ID` - Google Tag Manager container ID (environment variable)

2. **IAM Role Permissions**:
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": [
           "s3:PutObject",
           "s3:DeleteObject",
           "s3:ListBucket"
         ],
         "Resource": [
           "arn:aws:s3:::your-docs-bucket/*",
           "arn:aws:s3:::your-docs-bucket"
         ]
       }
     ]
   }
   ```

#### Deploying

1. Go to **Actions** tab in GitHub
2. Select **Deploy Documentation to S3** workflow
3. Click **Run workflow**
4. Choose environment (production/staging)
5. Click **Run workflow**

The workflow will:
- Build static site (`pnpm run build`)
- Export to `out/` directory with `index.html` at root
- Sync to S3 with `--delete` flag (removes old files)
- Invalidate CloudFront cache (if configured)

#### Local Build Test

Test the production build locally:

```bash
pnpm run build
cd out
python3 -m http.server 8000
```

Open [http://localhost:8000](http://localhost:8000) to verify.

### Other Deployment Options

The site can also be deployed to Vercel, Netlify, or other platforms supporting Next.js static export.

## Contributing

- Keep pages concise and focused
- Use code blocks with language tags for syntax highlighting
- Link to other pages using relative paths: `/docs/concepts/architecture`
- Add images to `public/images/` and reference as `/images/your-image.png`

## Troubleshooting

- If styles or navigation aren't updating, clear `.next/` and restart dev server
- Check `pnpm-lock.yaml` is committed for reproducible builds
- Ensure `.next/` is in `.gitignore`

## Learn More

- [Nextra Documentation](https://nextra.site/)
- [Next.js App Router](https://nextjs.org/docs/app)
