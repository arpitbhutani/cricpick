'use client';

import { useState, useEffect } from 'react';
import Link from 'next/link';
import { ShieldCheck, Globe, Loader2, AlertTriangle } from 'lucide-react';

interface Tournament {
  name: string;
  type: 'Franchise' | 'International' | string; // Allow string for flexibility if API changes
}

export default function TournamentsPage() {
  const [tournaments, setTournaments] = useState<Tournament[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchTournaments = async () => {
      setIsLoading(true);
      setError(null);
      try {
        const response = await fetch('/api/tournaments');
        if (!response.ok) {
          const errorData = await response.json();
          throw new Error(errorData.detail || `Error: ${response.status}`);
        }
        const data: Tournament[] = await response.json();
        setTournaments(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'An unknown error occurred');
        setTournaments([]);
      }
      setIsLoading(false);
    };

    fetchTournaments();
  }, []);

  const franchiseTournaments = tournaments.filter(t => t.type === 'Franchise');
  const internationalTournaments = tournaments.filter(t => t.type === 'International');
  const otherTournaments = tournaments.filter(t => t.type !== 'Franchise' && t.type !== 'International');

  if (isLoading) {
    return (
      <div className="flex justify-center items-center min-h-[calc(100vh-200px)]">
        <Loader2 className="h-12 w-12 animate-spin text-accent" />
        <p className="ml-4 text-xl text-textSecondary">Loading Tournaments...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="max-w-md mx-auto bg-red-100 border border-red-400 text-red-700 px-6 py-4 rounded-lg shadow-md">
        <div className="flex items-center">
          <AlertTriangle size={28} className="mr-3 text-red-500" />
          <h3 className="text-xl font-semibold">Error Fetching Tournaments</h3>
        </div>
        <p className="mt-2 text-sm">{error}</p>
        <p className="mt-3 text-xs">Please try refreshing the page or check back later.</p>
      </div>
    );
  }

  return (
    <div className="container mx-auto px-4 py-8">
      <h1 className="text-4xl font-bold text-center mb-12 text-primary">Explore Cricket Tournaments</h1>

      {(franchiseTournaments.length > 0 || internationalTournaments.length > 0 || otherTournaments.length > 0) ? (
        <>
          {franchiseTournaments.length > 0 && (
            <section className="mb-12">
              <h2 className="text-3xl font-semibold mb-6 text-accent flex items-center">
                <ShieldCheck size={32} className="mr-3" /> Franchise Leagues
              </h2>
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                {franchiseTournaments.map(t => (
                  <TournamentCard key={t.name} tournament={t} />
                ))}
              </div>
            </section>
          )}

          {internationalTournaments.length > 0 && (
            <section className="mb-12">
              <h2 className="text-3xl font-semibold mb-6 text-accent flex items-center">
                <Globe size={32} className="mr-3" /> International Cricket
              </h2>
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                {internationalTournaments.map(t => (
                  <TournamentCard key={t.name} tournament={t} />
                ))}
              </div>
            </section>
          )}
          
          {otherTournaments.length > 0 && (
             <section className="mb-12">
              <h2 className="text-3xl font-semibold mb-6 text-gray-600 flex items-center">
                 Other Tournaments
              </h2>
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                {otherTournaments.map(t => (
                  <TournamentCard key={t.name} tournament={t} />
                ))}
              </div>
            </section>
          )}
        </>
      ) : (
        <p className="text-center text-xl text-textSecondary mt-10">No tournaments found at the moment. Please check back later.</p>
      )}
    </div>
  );
}

interface TournamentCardProps {
  tournament: Tournament;
}

function TournamentCard({ tournament }: TournamentCardProps) {
  return (
    <Link href={`/props-analyzer?tournament=${encodeURIComponent(tournament.name)}`} className="block group">
      <div className="bg-card p-6 rounded-xl shadow-lg hover:shadow-xl transition-shadow duration-300 ease-in-out transform hover:-translate-y-1 cursor-pointer h-full flex flex-col justify-between">
        <div>
          <div className="flex items-center mb-3">
            {tournament.type === 'Franchise' ? 
              <ShieldCheck size={28} className="text-highlight mr-3 flex-shrink-0" /> : 
              <Globe size={28} className="text-highlight mr-3 flex-shrink-0" />}
            <h3 className="text-xl font-semibold text-primary truncate" title={tournament.name}>{tournament.name}</h3>
          </div>
          <p className="text-sm text-textSecondary">Type: {tournament.type}</p>
        </div>
        {/* Add more details if available, e.g., number of teams, current season */}
        <div className="mt-4 text-right">
            <span className="text-xs font-semibold text-blue-600 group-hover:underline">Analyze Props &rarr;</span>
        </div>
      </div>
    </Link>
  );
} 