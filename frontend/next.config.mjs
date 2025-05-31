/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  // If your API is on a different domain or port during development,
  // you might want to set up rewrites here to avoid CORS issues.
  // Example:
  async rewrites() {
    const backendApiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://127.0.0.1:8000';
    return [
      {
        source: '/api/:path*',
        // Ensure the destination ends with /api/:path* if your backend URL already includes /api
        // If NEXT_PUBLIC_API_URL is https://cricpick.onrender.com, then destination should be https://cricpick.onrender.com/api/:path*
        // If NEXT_PUBLIC_API_URL is https://cricpick.onrender.com/api, then destination should be https://cricpick.onrender.com/:path*
        // Assuming NEXT_PUBLIC_API_URL will be set to the base (e.g. https://cricpick.onrender.com) and /api is part of the path.
        destination: `${backendApiUrl}/api/:path*`,
      },
    ]
  },
};

export default nextConfig; 