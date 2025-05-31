'use client';

import { useState, useEffect, FormEvent } from 'react';
import Link from 'next/link';
import { UserSearch, Users, Loader2, AlertTriangle, ArrowRight } from 'lucide-react';

interface Player {
  name: string;
}

export default function PlayerSearchPage() {
  const [searchTerm, setSearchTerm] = useState('');
  const [players, setPlayers] = useState<Player[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [searched, setSearched] = useState(false);

  const handleSearch = async (event?: FormEvent<HTMLFormElement>) => {
    if (event) event.preventDefault();
    if (!searchTerm.trim()) {
      setPlayers([]);
      setSearched(false);
      return;
    }
    setIsLoading(true);
    setError(null);
    setSearched(true);
    try {
      const response = await fetch(`/api/players?q=${encodeURIComponent(searchTerm)}&limit=50`);
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || `Error fetching players: ${response.status}`);
      }
      const data: Player[] = await response.json();
      setPlayers(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An unknown error occurred');
      setPlayers([]);
    }
    setIsLoading(false);
  };

  useEffect(() => {
    // Optional: trigger search on searchTerm change after a delay (debouncing)
    // For now, we rely on explicit search button click or form submission
  }, [searchTerm]);

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="max-w-2xl mx-auto bg-card p-6 md:p-8 rounded-xl shadow-2xl">
        <h1 className="text-4xl font-bold text-center mb-6 text-primary flex items-center justify-center">
          <UserSearch size={40} className="mr-3 text-accent" /> Player Prop Search
        </h1>
        <form onSubmit={handleSearch} className="flex flex-col sm:flex-row gap-4 mb-8">
          <input
            type="text"
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            placeholder="Enter player name (e.g., Virat Kohli)"
            className="flex-grow px-4 py-3 border border-border rounded-lg focus:ring-2 focus:ring-accent focus:border-transparent outline-none shadow-sm transition-shadow focus:shadow-md bg-background text-textPrimary placeholder-textSecondary"
          />
          <button 
            type="submit" 
            disabled={isLoading || !searchTerm.trim()}
            className="bg-accent hover:bg-highlight text-white font-semibold py-3 px-6 rounded-lg shadow-md hover:shadow-lg transition-all duration-300 ease-in-out disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center"
          >
            {isLoading ? (
              <><Loader2 className="animate-spin h-5 w-5 mr-2" /> Searching...</>
            ) : (
              <><UserSearch size={20} className="mr-2" /> Search</>
            )}
          </button>
        </form>

        {isLoading && (
          <div className="flex justify-center items-center py-6">
            <Loader2 className="h-10 w-10 animate-spin text-accent" />
            <p className="ml-3 text-lg text-textSecondary">Loading players...</p>
          </div>
        )}

        {error && (
          <div className="bg-red-100 border-l-4 border-red-500 text-red-700 p-4 rounded-md shadow-md" role="alert">
            <div className="flex items-center">
              <AlertTriangle size={24} className="mr-3" />
              <p className="font-semibold">Error:</p>
            </div>
            <p className="text-sm ml-8">{error}</p>
          </div>
        )}

        {!isLoading && searched && players.length === 0 && !error && (
          <div className="text-center py-6">
            <Users size={48} className="mx-auto text-textSecondary mb-3" />
            <p className="text-xl text-textSecondary">No players found matching "{searchTerm}".</p>
            <p className="text-sm text-textTertiary">Try a different name or check for typos.</p>
          </div>
        )}

        {players.length > 0 && (
          <div className="mt-6 flow-root">
            <ul role="list" className="-my-5 divide-y divide-border">
              {players.map((player) => (
                <li key={player.name} className="py-5">
                  <Link 
                    href={`/players/${encodeURIComponent(player.name)}/props`}
                    className="group block p-4 rounded-lg hover:bg-hover transition-colors duration-200 ease-in-out"
                  >
                    <div className="flex items-center justify-between">
                      <p className="text-lg font-medium text-primary group-hover:text-accent truncate" title={player.name}>
                        {player.name}
                      </p>
                      <div className="ml-4 flex-shrink-0">
                        <ArrowRight size={20} className="text-textSecondary group-hover:text-accent transition-transform duration-200 ease-in-out group-hover:translate-x-1" />
                      </div>
                    </div>
                  </Link>
                </li>
              ))}
            </ul>
            <p className='text-sm text-textTertiary text-center mt-6'>Showing top {players.length} results for "{searchTerm}".</p>
          </div>
        )}
      </div>
    </div>
  );
} 