'use client';

import { useState, useEffect } from 'react';
import Link from 'next/link';
import { Users, ListFilter, Loader2, AlertTriangle, Shield } from 'lucide-react';

interface Team {
  name: string;
  // Potentially add more fields if API provides, e.g., country, logo_url
}

interface Tournament {
  name: string;
  type: string;
}

export default function TeamsPage() {
  const [allTeams, setAllTeams] = useState<Team[]>([]);
  const [filteredTeams, setFilteredTeams] = useState<Team[]>([]);
  const [tournaments, setTournaments] = useState<Tournament[]>([]);
  const [selectedTournaments, setSelectedTournaments] = useState<string[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      setIsLoading(true);
      setError(null);
      try {
        const [teamsResponse, tournamentsResponse] = await Promise.all([
          fetch('/api/teams/list'),
          fetch('/api/tournaments')
        ]);

        if (!teamsResponse.ok) {
          const errorData = await teamsResponse.json();
          throw new Error(`Error fetching teams: ${errorData.detail || teamsResponse.status}`);
        }
        if (!tournamentsResponse.ok) {
          const errorData = await tournamentsResponse.json();
          throw new Error(`Error fetching tournaments: ${errorData.detail || tournamentsResponse.status}`);
        }

        const teamsData: Team[] = await teamsResponse.json();
        const tournamentsData: Tournament[] = await tournamentsResponse.json();
        
        setAllTeams(teamsData);
        setFilteredTeams(teamsData); // Initially show all teams
        setTournaments(tournamentsData.sort((a,b) => a.name.localeCompare(b.name)));

      } catch (err) {
        setError(err instanceof Error ? err.message : 'An unknown error occurred');
        setAllTeams([]);
        setFilteredTeams([]);
        setTournaments([]);
      }
      setIsLoading(false);
    };
    fetchData();
  }, []);

  useEffect(() => {
    const fetchFilteredTeams = async () => {
      if (selectedTournaments.length === 0) {
        setFilteredTeams(allTeams);
        return;
      }
      setIsLoading(true);
      setError(null);
      try {
        const response = await fetch(`/api/teams/list?tournaments=${selectedTournaments.join(',')}`);
        if (!response.ok) {
          const errorData = await response.json();
          throw new Error(`Error fetching filtered teams: ${errorData.detail || response.status}`);
        }
        const data: Team[] = await response.json();
        setFilteredTeams(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'An unknown error occurred');
        setFilteredTeams([]);
      }
      setIsLoading(false);
    };

    if(allTeams.length > 0) { // Only fetch if initial teams are loaded and selection changes
        fetchFilteredTeams();
    }
  }, [selectedTournaments, allTeams]);

  const handleTournamentSelection = (tournamentName: string) => {
    setSelectedTournaments(prev => 
      prev.includes(tournamentName) 
        ? prev.filter(name => name !== tournamentName)
        : [...prev, tournamentName]
    );
  };

  if (isLoading && allTeams.length === 0) { // Show initial loading spinner only
    return (
      <div className="flex justify-center items-center min-h-[calc(100vh-200px)]">
        <Loader2 className="h-12 w-12 animate-spin text-accent" />
        <p className="ml-4 text-xl text-textSecondary">Loading Teams...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="max-w-md mx-auto bg-red-100 border border-red-400 text-red-700 px-6 py-4 rounded-lg shadow-md">
        <div className="flex items-center">
          <AlertTriangle size={28} className="mr-3 text-red-500" />
          <h3 className="text-xl font-semibold">Error Fetching Data</h3>
        </div>
        <p className="mt-2 text-sm">{error}</p>
      </div>
    );
  }

  return (
    <div className="container mx-auto px-4 py-8">
      <h1 className="text-4xl font-bold text-center mb-6 text-primary">Cricket Teams</h1>
      
      <div className="mb-8 p-6 bg-card rounded-xl shadow-lg">
        <h2 className="text-2xl font-semibold mb-4 text-accent flex items-center">
          <ListFilter size={28} className="mr-3" /> Filter by Tournament
        </h2>
        {tournaments.length > 0 ? (
          <div className="flex flex-wrap gap-3">
            {tournaments.map(t => (
              <button 
                key={t.name} 
                onClick={() => handleTournamentSelection(t.name)}
                className={`px-4 py-2 rounded-full text-sm font-medium transition-colors duration-200 ease-in-out border-2 
                  ${selectedTournaments.includes(t.name) 
                    ? 'bg-accent text-white border-accent'
                    : 'bg-transparent text-accent border-accent hover:bg-accent/10'}`}
              >
                {t.name}
              </button>
            ))}
          </div>
        ) : (
          <p className="text-textSecondary">No tournaments available for filtering.</p>
        )}
        {selectedTournaments.length > 0 && (
            <button 
                onClick={() => setSelectedTournaments([])}
                className='mt-4 text-sm text-highlight hover:underline'
            >
                Clear all filters
            </button>
        )}
      </div>

      {isLoading && <div className="flex justify-center my-6"><Loader2 className="h-8 w-8 animate-spin text-accent" /></div>}

      {filteredTeams.length > 0 ? (
        <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-6">
          {filteredTeams.map(team => (
            <TeamCard key={team.name} team={team} />
          ))}
        </div>
      ) : (
        !isLoading && <p className="text-center text-xl text-textSecondary mt-10">No teams found matching your criteria.</p>
      )}
    </div>
  );
}

interface TeamCardProps {
  team: Team;
}

function TeamCard({ team }: TeamCardProps) {
  return (
    // TODO: Link to team detail page: /teams/[teamName]
    // <Link href={`/teams/${encodeURIComponent(team.name)}`} className="block group">
      <div className="bg-card p-6 rounded-xl shadow-lg hover:shadow-xl transition-shadow duration-300 ease-in-out transform hover:-translate-y-1 cursor-pointer flex flex-col items-center">
        <Shield size={40} className="text-highlight mb-4" /> 
        <h3 className="text-xl font-semibold text-primary text-center truncate w-full" title={team.name}>{team.name}</h3>
        {/* Potential future details: Country Flag, number of players */}
      </div>
    // </Link>
  );
} 