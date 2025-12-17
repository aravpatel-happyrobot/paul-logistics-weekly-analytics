import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: "Paul Logistics Analytics",
  description: "Daily analytics dashboard for Paul Logistics",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className={inter.className}>
        <div className="min-h-screen">
          {/* Header */}
          <header className="sticky top-0 z-50 glass border-b border-gray-200/50">
            <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
              <div className="flex justify-between items-center h-16">
                <div className="flex items-center gap-4">
                  <div className="relative">
                    <div className="w-10 h-10 gradient-primary rounded-xl flex items-center justify-center shadow-lg shadow-indigo-500/30">
                      <svg
                        className="w-6 h-6 text-white"
                        fill="none"
                        stroke="currentColor"
                        viewBox="0 0 24 24"
                      >
                        <path
                          strokeLinecap="round"
                          strokeLinejoin="round"
                          strokeWidth={2}
                          d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"
                        />
                      </svg>
                    </div>
                  </div>
                  <div>
                    <h1 className="text-xl font-bold text-gray-900">
                      Paul Logistics
                    </h1>
                    <p className="text-xs text-gray-500 -mt-0.5">Analytics Dashboard</p>
                  </div>
                </div>
                <nav className="flex items-center gap-1">
                  <a
                    href="/"
                    className="px-4 py-2 text-sm font-medium text-gray-700 hover:text-indigo-600 hover:bg-indigo-50 rounded-lg transition-all duration-200"
                  >
                    Dashboard
                  </a>
                  <a
                    href="/reports"
                    className="px-4 py-2 text-sm font-medium text-gray-500 hover:text-indigo-600 hover:bg-indigo-50 rounded-lg transition-all duration-200"
                  >
                    Reports
                  </a>
                  <div className="ml-4 pl-4 border-l border-gray-200">
                    <div className="flex items-center gap-2 px-3 py-1.5 bg-green-50 rounded-full">
                      <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse"></div>
                      <span className="text-xs font-medium text-green-700">Live</span>
                    </div>
                  </div>
                </nav>
              </div>
            </div>
          </header>

          {/* Main Content */}
          <main className="pb-12">{children}</main>

          {/* Footer */}
          <footer className="fixed bottom-0 left-0 right-0 py-3 glass border-t border-gray-200/50">
            <div className="max-w-7xl mx-auto px-4 flex justify-between items-center">
              <p className="text-xs text-gray-500">
                Powered by HappyRobot AI
              </p>
              <p className="text-xs text-gray-400">
                Auto-updates daily at 6:00 AM
              </p>
            </div>
          </footer>
        </div>
      </body>
    </html>
  );
}
