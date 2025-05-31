/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  // If your API is on a different domain or port during development,
  // you might want to set up rewrites here to avoid CORS issues.
  // Example:
  async rewrites() {
    return [
      {
        source: '/api/:path*',
        destination: 'http://127.0.0.1:8000/api/:path*', // Your FastAPI backend
      },
    ]
  },
};

export default nextConfig; 