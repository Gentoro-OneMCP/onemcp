import nextra from "nextra";

// Set up Nextra with its configuration
const withNextra = nextra({
	// ... Add Nextra-specific options here
  defaultShowCopyCode: true
});

// Export the final Next.js config with Nextra included
export default withNextra({
	// ... Add regular Next.js options here
	output: 'export',
	images: {
		unoptimized: true,
	},
    redirects: async () => [
        {
            source: '/docs/installation',
            destination: '/docs/installation/oci-image',
            permanent: false,
        },
        {
            source: '/docs/setup',
            destination: '/docs/setup/handbook',
            permanent: false,
        },
    ]
});
