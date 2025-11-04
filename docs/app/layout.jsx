import { Footer, Layout, Navbar } from "nextra-theme-docs";
import { Banner, Head } from "nextra/components";
import { getPageMap } from "nextra/page-map";
import "nextra-theme-docs/style.css";
import "./globals.css";
import Script from "next/script";

export const metadata = {
	// Define your metadata here
	// For more information on metadata API, see: https://nextjs.org/docs/app/building-your-application/optimizing/metadata
};

const banner = (
	<Banner storageKey="gentoro-onemcp-banner">
		🚀 Gentoro OneMCP is now open source!
	</Banner>
);
const navbar = (
	<Navbar
		logo={<b>OneMCP</b>}
		projectLink="https://github.com/gentoro-GT/onemcp"
	// ... Your additional navbar options
	/>
);
const footer = <Footer>MIT {new Date().getFullYear()} © Gentoro.</Footer>;

export default async function RootLayout({ children }) {
	const gtmId = process.env.NEXT_PUBLIC_GTM_ID;
	return (
		<html
			// Not required, but good for SEO
			lang="en"
			// Required to be set
			dir="ltr"
			// Suggested by `next-themes` package https://github.com/pacocoursey/next-themes#with-app
			suppressHydrationWarning
		>
			<Head
			// ... Your additional head options
			>
				{/* Your additional tags should be passed as `children` of `<Head>` element */}
			</Head>
			<body>
				{gtmId ? (
					<>
						<Script id="consent-default" strategy="beforeInteractive">
							{`window.dataLayer = window.dataLayer || [];
								function gtag(){dataLayer.push(arguments);}
								gtag('consent', 'default', {
									'analytics_storage': 'granted',
									'ad_storage': 'granted',
									'ad_user_data': 'granted',
									'ad_personalization': 'granted'
								});`}
						</Script>
						<Script id="gtm" strategy="afterInteractive">
							{`(function(w,d,s,l,i){w[l]=w[l]||[];
								w[l].push({'gtm.start':new Date().getTime(),event:'gtm.js'});
								var f=d.getElementsByTagName(s)[0],j=d.createElement(s),dl=l!='dataLayer'?'&l='+l:'';
								j.async=true;j.src='https://www.googletagmanager.com/gtm.js?id='+i+dl;
								f.parentNode.insertBefore(j,f);
								})(window,document,'script','dataLayer','${gtmId}');`}
						</Script>
						<noscript>
							<iframe
								src={`https://www.googletagmanager.com/ns.html?id=${gtmId}`}
								height="0"
								width="0"
								style={{ display: "none", visibility: "hidden" }}
							/>
						</noscript>
					</>
				) : null}
				<Layout
					banner={banner}
					navbar={navbar}
					pageMap={await getPageMap()}
					docsRepositoryBase="https://github.com/gentoro-GT/onemcp/tree/main/docs"
					editLink="Edit this page on GitHub"
					footer={footer}
				// ... Your additional layout options
				>
					{children}
				</Layout>
			</body>
		</html>
	);
}
