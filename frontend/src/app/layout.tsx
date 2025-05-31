import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";
import Link from 'next/link';
import { Home, BarChart3, Search } from 'lucide-react'; // Example icons

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: "Cristat",
  description: "T20 Cricket Stats Analysis",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className={`${inter.className} bg-black text-neutral-100 min-h-screen flex flex-col`}>
        <header className="bg-neutral-900 shadow-md sticky top-0 z-50">
          <nav className="container mx-auto px-6 py-3">
            <div className="flex items-center justify-between">
              <Link href="/" className="text-2xl font-bold text-primary hover:text-primary/80 transition-colors">
                Cristat
              </Link>
              <div className="flex items-center space-x-4">
                <Link href="/" className="flex items-center text-neutral-300 hover:text-primary transition-colors px-3 py-2 rounded-md text-sm font-medium">
                  <Home size={18} className="mr-2" />
                  Home
                </Link>
                <Link href="/props-analyzer" className="flex items-center text-neutral-300 hover:text-primary transition-colors px-3 py-2 rounded-md text-sm font-medium">
                  <BarChart3 size={18} className="mr-2" />
                  Player Props
                </Link>
                {/* Add other links here as needed */}
              </div>
            </div>
          </nav>
        </header>
        <main className="flex-grow container mx-auto p-4 md:p-8">
          {children}
        </main>
        <footer className="bg-neutral-900 text-center p-4 text-neutral-400 text-sm border-t border-neutral-800">
          © {new Date().getFullYear()} Cristat. All rights reserved.
        </footer>
      </body>
    </html>
  );
} 