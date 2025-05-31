import Link from 'next/link';
import { BarChart3, Users, Shield, TrendingUp, Zap, SlidersHorizontal } from 'lucide-react';

const FeatureCard = ({ title, description, icon, link }: { title: string, description: string, icon: React.ReactNode, link: string }) => (
  <Link href={link} className="block p-6 bg-neutral-900 hover:bg-neutral-800 rounded-xl shadow-lg transition-all duration-300 ease-in-out hover:shadow-primary/20 transform hover:-translate-y-1 border border-neutral-700 hover:border-primary/50">
    <div className="flex items-center justify-center mb-4 text-primary w-12 h-12 bg-primary/10 rounded-full">
      {icon}
    </div>
    <h3 className="text-xl font-semibold mb-2 text-neutral-100">{title}</h3>
    <p className="text-neutral-400 text-sm">{description}</p>
  </Link>
);

export default function HomePage() {
  const features = [
    {
      title: "Player Statistics",
      description: "Deep dive into individual player performance, career stats, and recent form.",
      icon: <BarChart3 size={24} />,
      link: "/players"
    },
    {
      title: "Team Analysis",
      description: "Explore team-based statistics, head-to-head records, and tournament performance.",
      icon: <Users size={24} />,
      link: "/teams"
    },
    {
      title: "Tournament Insights",
      description: "Compare tournaments, view historical data, and identify trends across different leagues.",
      icon: <Shield size={24} />,
      link: "/tournaments"
    },
    {
      title: "Player Props Analyzer",
      description: "Analyze player prop bets using historical data and advanced filters.",
      icon: <SlidersHorizontal size={24} />,
      link: "/props-analyzer"
    },
  ];

  return (
    <div className="min-h-screen flex flex-col items-center justify-center text-center p-4 md:p-8">
      <main className="max-w-4xl w-full">
        <div className="mb-12">
          <h1 className="text-5xl md:text-7xl font-bold mb-4">
            Welcome to <span className="text-primary">Cristat</span>
          </h1>
          <p className="text-xl md:text-2xl text-neutral-300 mb-8 max-w-2xl mx-auto">
            Your ultimate hub for T20 cricket statistics, player performance analysis, and prop betting insights.
          </p>
          <div className="flex justify-center space-x-4">
            <Link href="/props-analyzer" 
                  className="bg-primary hover:bg-primary/90 text-white font-semibold py-3 px-8 rounded-lg text-lg transition-transform transform hover:scale-105 shadow-lg hover:shadow-primary/50">
              Analyze Player Props
            </Link>
            <Link href="#features"
                  className="bg-neutral-700 hover:bg-neutral-600 text-neutral-100 font-semibold py-3 px-8 rounded-lg text-lg transition-transform transform hover:scale-105 shadow-lg">
              Explore Features
            </Link>
          </div>
        </div>

        <div id="features" className="grid grid-cols-1 md:grid-cols-2 gap-6 md:gap-8">
          {features.map((feature, index) => (
            <FeatureCard key={index} {...feature} />
          ))}
        </div>
      </main>
    </div>
  );
} 