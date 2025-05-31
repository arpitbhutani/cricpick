"use client";

import React, { useState, useEffect, useMemo, useCallback, Suspense } from 'react';
import dynamic from 'next/dynamic';
import { useSearchParams } from 'next/navigation'; // Import useSearchParams
import { Eye, EyeOff, Filter, Loader2, TrendingUp, Zap, BarChart3, Search, Globe, Building, ChevronDown, ChevronUp, ChevronsUpDown, Crosshair } from 'lucide-react'; // Import Eye, EyeOff, Filter, Loader2, TrendingUp, Zap, BarChart3, Search, Globe, Building icons

// Define types for our data
interface Tournament {
  name: string;
  type: string;
}

interface Team {
  name: string;
}

interface Venue { // Added Venue interface
  name: string;
}

// New Types for Team Aggregated Stats
interface TeamRunsThresholdStat {
  threshold: number;
  total_player_innings_for_criteria: number;
  total_player_innings_met_threshold: number;
  team_hit_rate_percentage: number;
}

interface TeamBattingStatsResponse {
  team_name: string;
  filters_applied: Record<string, any>; // Can be more specific if needed
  stats: TeamRunsThresholdStat[];
  contributing_players_count: number;
}

// For Individual Player Stats (Option B)
interface PlayerRunsThresholdStat {
  threshold: number;
  total_innings_for_criteria: number;
  innings_met_or_exceeded_threshold: number;
  hit_rate_percentage: number;
}

interface PlayerGamelogEntry { // Simplified for now, expand if gamelog is shown for individuals
    match_id: string;
    match_date: string; // or Date
    batting_team: string;
    bowling_team: string;
    city: string | null;
    innings_no: number;
    runs_scored: number;
    balls_faced: number;
    fours: number;
    sixes: number;
    wicket_type?: string | null;
    wicket_bowler?: string | null;
    strike_rate?: number | null;
    // Specific to boundary props if ever combined
    boundaries_hit_in_inning?: number;
}

interface BowlerGamelogEntry {
    match_id: string;
    match_date: string; // or Date
    bowling_team_for_player: string; // Team the player (bowler) is playing for
    batting_team_opponent: string; // Team the player bowled against
    city: string | null;
    innings_no: number;
    wickets_taken_in_inning: number;
    runs_conceded_in_inning: number;
    balls_bowled_in_inning: number;
    economy_rate_in_inning?: number | null;
    overs_bowled?: string; // e.g., "4.0", "3.5"
}

// Type guard for BowlerGamelogEntry
function isBowlerGamelogEntry(log: PlayerGamelogEntry | BowlerGamelogEntry): log is BowlerGamelogEntry {
  return 'wickets_taken_in_inning' in log;
}

interface IndividualPlayerRunsStats {
  player_name: string;
  stats: PlayerRunsThresholdStat[];
  gamelog?: PlayerGamelogEntry[] | null; // Optional gamelog for players
}

// For Individual Player Boundary Stats
interface PlayerBoundaryThresholdStat {
  threshold: number;
  total_innings_for_criteria: number;
  innings_met_or_exceeded_threshold: number;
  hit_rate_percentage: number;
}

interface IndividualPlayerBoundaryStats {
  player_name: string;
  boundary_type: '4s' | '6s'; // To know which boundary we are looking at
  stats: PlayerBoundaryThresholdStat[];
  gamelog?: PlayerGamelogEntry[] | null; // Re-using PlayerGamelogEntry if needed
}

interface PlayerStatThreshold {
  threshold: number;
  total_innings_for_criteria: number;
  innings_met_or_exceeded_threshold: number;
  hit_rate_percentage: number;
}

interface PlayerStats { // Consolidated PlayerStats type
  player_name: string;
  stats: PlayerRunsThresholdStat[];
  gamelog?: PlayerGamelogEntry[] | BowlerGamelogEntry[] | null; // Updated to support both types
  boundary_type?: '4s' | '6s'; // Optional: to help distinguish if needed, though PlayerStats is generic
}

const PropsAnalyzerContent: React.FC = () => {
  const searchParams = useSearchParams(); // Hook to get URL search params
  const [allTournaments, setAllTournaments] = useState<Tournament[]>([]); // Stores all fetched tournaments
  const [tournaments, setTournaments] = useState<Tournament[]>([]); // Filtered tournaments for display
  const [selectedTournamentType, setSelectedTournamentType] = useState<string>('all'); // 'all', 'Franchise', 'International'
  const [selectedTournament, setSelectedTournament] = useState<string>('');
  const [teams, setTeams] = useState<Team[]>([]);
  const [selectedTeam, setSelectedTeam] = useState<string>('');
  const [selectedOpposition, setSelectedOpposition] = useState<string>('');
  const [venuesList, setVenuesList] = useState<Venue[]>([]);
  const [selectedVenue, setSelectedVenue] = useState<string>('');
  const [playerStats, setPlayerStats] = useState<PlayerStats[]>([]); // Using consolidated type
  const [activeStatsView, setActiveStatsView] = useState<'runs' | '4s' | '6s' | 'wickets'>('runs');
  const [thresholds, setThresholds] = useState<string>('10,20,30,50,75,100');
  const [loadingTournaments, setLoadingTournaments] = useState<boolean>(true);
  const [loadingTeams, setLoadingTeams] = useState<boolean>(false);
  const [loadingVenues, setLoadingVenues] = useState<boolean>(false);
  const [loadingStats, setLoadingStats] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [lastNSeasons, setLastNSeasons] = useState<string>('');
  const [minPlayerInnings, setMinPlayerInnings] = useState<string>('5');
  const [expandedGamelogs, setExpandedGamelogs] = useState<Record<string, boolean>>({});
  const [sortConfig, setSortConfig] = useState<{ key: string | number | null; direction: 'ascending' | 'descending' }>({ key: null, direction: 'ascending' });

  // Effect 1: Fetch all tournaments on component mount
  useEffect(() => {
    const fetchTournaments = async () => {
      setLoadingTournaments(true);
      try {
        const res = await fetch('/api/tournaments');
        if (!res.ok) throw new Error('Failed to fetch tournaments');
        const data: Tournament[] = await res.json();
        setAllTournaments(data);
        // Initialize displayed tournaments based on default type 'all'
        setTournaments(data); 
        const queryTournament = searchParams.get('tournament');
        if (queryTournament && data.some(t => t.name === queryTournament)) {
          const tournamentDetails = data.find(t => t.name === queryTournament);
          if (tournamentDetails) {
            setSelectedTournamentType(tournamentDetails.type); // Set type based on query param
            setSelectedTournament(queryTournament);
          }
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : 'An unknown error occurred');
        setAllTournaments([]);
        setTournaments([]);
      } finally {
        setLoadingTournaments(false);
      }
    };
    fetchTournaments();
  }, [searchParams]);

  // Effect 2: Filter tournaments when selectedTournamentType changes
  useEffect(() => {
    if (selectedTournamentType === 'all') {
      setTournaments(allTournaments);
    } else {
      setTournaments(allTournaments.filter(t => t.type === selectedTournamentType));
    }
    setSelectedTournament(''); // Reset selected tournament when type changes
    // Dependent data (teams, venues, stats) will be reset/refetched by subsequent effects or actions
  }, [selectedTournamentType, allTournaments]);

  // Effect 3: Fetch teams AND VENUES when a tournament is selected OR type is selected (and no specific tournament)
  const fetchTeamsAndVenues = useCallback(async () => {
    let tournamentQueryValue = '';

    if (selectedTournament) {
      tournamentQueryValue = selectedTournament;
    } else if (selectedTournamentType !== 'all') {
      const relevantTournaments = allTournaments.filter(t => t.type === selectedTournamentType).map(t => t.name);
      if (relevantTournaments.length > 0) {
        tournamentQueryValue = relevantTournaments.join(',');
      } else {
        // No tournaments of the selected type, clear teams/venues
        setTeams([]);
        setVenuesList([]);
        setLoadingTeams(false);
        setLoadingVenues(false);
        return;
      }
    } else {
      // "All Types" and no specific tournament selected - might mean all teams/venues or handle as per desired UX
      // For now, let's assume if no specific tournament and type is 'all', we fetch all teams/venues (empty tournament query)
      // This behavior can be refined. If it should fetch for ALL tournaments, the query needs to be empty.
      // If it should wait for a tournament, then this block might not run.
      // Current API list_teams and list_venues will fetch ALL if tournaments param is empty.
    }

    if (!selectedTournament && selectedTournamentType === 'all' && !searchParams.get('tournament')) {
        // If "All Types" is selected and no specific tournament, don't fetch teams/venues immediately.
        // Let user pick a specific tournament or a more specific type.
        setTeams([]);
        setVenuesList([]);
        setSelectedTeam('');
        setSelectedOpposition('');
        setSelectedVenue('');
        setPlayerStats([]);
        return;
    }
    
    setLoadingTeams(true);
    setLoadingVenues(true);
    setTeams([]);
    setVenuesList([]);
    // Keep selectedTeam, selectedOpposition, selectedVenue as they might be relevant if user is just refining tournament
    setError(null);
    // setPlayerStats([]); // Don't clear stats here, let fetchStats handle it

    const fetchPath = tournamentQueryValue ? `?tournaments=${encodeURIComponent(tournamentQueryValue)}` : '';

    try {
      const teamRes = await fetch(`/api/teams/list${fetchPath}`);
      if (!teamRes.ok) throw new Error('Failed to fetch teams');
      const teamData: Team[] = await teamRes.json();
      setTeams(teamData);
    } catch (err) {
      setError(prevError => prevError ? `${prevError}, ${err instanceof Error ? err.message : 'Failed to fetch teams'}` : (err instanceof Error ? err.message : 'Failed to fetch teams'));
      setTeams([]);
    } finally {
      setLoadingTeams(false);
    }

    try {
      const venueRes = await fetch(`/api/venues${fetchPath}`);
      if (!venueRes.ok) throw new Error('Failed to fetch venues');
      const venueData: Venue[] = await venueRes.json();
      setVenuesList(venueData);
    } catch (err) {
      setError(prevError => prevError ? `${prevError}, ${err instanceof Error ? err.message : 'Failed to fetch venues'}` : (err instanceof Error ? err.message : 'Failed to fetch venues'));
      setVenuesList([]);
    } finally {
      setLoadingVenues(false);
    }
  }, [selectedTournament, selectedTournamentType, allTournaments, searchParams]);

  useEffect(() => {
    // This effect now triggers based on selectedTournament OR selectedTournamentType
    // Reset dependent selections when the primary tournament context changes
    if (!selectedTournament && selectedTournamentType === 'all') {
        // If "All Types" is selected and no specific tournament, ensure dependent fields are clear
        setSelectedTeam('');
        setSelectedOpposition('');
        setSelectedVenue('');
        setTeams([]); // Clear teams list
        setVenuesList([]); // Clear venues list
        setPlayerStats([]); // Clear stats
    } else {
         // If a specific tournament is chosen, or a type is chosen, fetch teams/venues.
        // We also need to reset team/opposition/venue if the tournament/type changes fundamentally.
        // The fetchTeamsAndVenues itself doesn't reset these, so we do it here before calling it.
        const previousTournament = searchParams.get('tournament'); // A bit of a hack to see if it's an initial load with a param
        if(selectedTournament || (selectedTournamentType !== 'all' && !previousTournament) ) {
            setSelectedTeam('');
            setSelectedOpposition('');
            setSelectedVenue('');
        }
        fetchTeamsAndVenues();
    }
  }, [selectedTournament, selectedTournamentType, fetchTeamsAndVenues, searchParams]);

  const handleFetchStats = async () => {
    // Determine tournament query parameter
    let tournamentQueryParam = '';
    if (selectedTournament) {
      tournamentQueryParam = selectedTournament;
    } else if (selectedTournamentType !== 'all') {
      const relevantTournaments = allTournaments.filter(t => t.type === selectedTournamentType).map(t => t.name);
      if (relevantTournaments.length > 0) {
        tournamentQueryParam = relevantTournaments.join(',');
      }
    }
    // If tournamentQueryParam is still empty here, it means "All Types" and no specific tournament.
    // The API might interpret an empty 'tournament' param as "all tournaments overall"
    // or we might require a tournament/type to be selected.
    // For now, let's proceed; if tournamentQueryParam is empty, the API call won't filter by tournament.

    if (!tournamentQueryParam && !selectedTeam) {
      setError("Please select a tournament, a tournament type, or a team to fetch stats.");
      setPlayerStats([]);
      return;
    }

    setLoadingStats(true);
    setError(null);
    setPlayerStats([]);
    setExpandedGamelogs({});

    try {
      const queryParams = new URLSearchParams();
      if (tournamentQueryParam) queryParams.append('tournament', tournamentQueryParam); // Use the determined param
      if (selectedTeam) queryParams.append('team', selectedTeam);
      if (selectedOpposition) queryParams.append('opposition', selectedOpposition);
      if (selectedVenue) queryParams.append('venue', selectedVenue);
      if (thresholds) queryParams.append('thresholds', thresholds);
      if (minPlayerInnings) queryParams.append('min_innings', minPlayerInnings);
      if (lastNSeasons) {
        queryParams.append('last_n', lastNSeasons);
      }
      queryParams.append('include_gamelog', 'true');

      let endpoint = '';
      if (activeStatsView === 'runs') {
        endpoint = '/api/batters/stats/runs_hit_rates';
      } else if (activeStatsView === '4s') {
        endpoint = '/api/batters/stats/boundary_hit_rates';
        queryParams.append('boundary_type', '4s');
      } else if (activeStatsView === '6s') {
        endpoint = '/api/batters/stats/boundary_hit_rates';
        queryParams.append('boundary_type', '6s');
      } else if (activeStatsView === 'wickets') {
        endpoint = '/api/bowlers/stats/wickets_taken_rates';
      }

      const res = await fetch(`${endpoint}?${queryParams.toString()}`);
      if (!res.ok) {
        const errorData = await res.json();
        throw new Error(errorData.detail || `Failed to fetch ${activeStatsView} stats`);
      }
      const data: PlayerStats[] = await res.json();
      data.sort((a, b) => a.player_name.localeCompare(b.player_name));
      setPlayerStats(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : `An unknown error occurred while fetching ${activeStatsView} stats`);
      setPlayerStats([]);
    } finally {
      setLoadingStats(false);
    }
  };
  
  const toggleGamelog = (playerName: string) => {
    setExpandedGamelogs(prev => ({ ...prev, [playerName]: !prev[playerName] }));
  };

  const getStatTypeLabel = (statType: 'runs' | '4s' | '6s' | 'wickets') => {
    if (statType === 'runs') return 'Runs';
    if (statType === '4s') return '4s';
    if (statType === '6s') return '6s';
    if (statType === 'wickets') return 'Wkts';
    return '';
  };

  const handleStatViewChange = (view: 'runs' | '4s' | '6s' | 'wickets') => {
    setActiveStatsView(view);
    setPlayerStats([]); 
    setError(null); 
    setExpandedGamelogs({});
    if (view === 'runs') {
      setThresholds('10,20,30,50,75,100');
    } else if (view === '4s') {
      setThresholds('1,2,3,4,5');
    } else if (view === '6s') {
      setThresholds('1,2,3,4,5');
    } else if (view === 'wickets') {
      setThresholds('1,2,3,4'); // Default thresholds for wickets
    }
  };

  const handleTournamentTypeChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    setSelectedTournamentType(e.target.value);
    // Reset dependent filters because the list of tournaments will change
    setSelectedTournament('');
    setSelectedTeam('');
    setSelectedOpposition('');
    setSelectedVenue('');
    setTeams([]);
    setVenuesList([]);
    setPlayerStats([]); // Also clear stats
  };

  const handleTournamentChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    setSelectedTournament(e.target.value);
    // Reset team and venue as they are dependent on the specific tournament
    setSelectedTeam('');
    setSelectedOpposition('');
    setSelectedVenue('');
    // Teams and venues will be fetched by the useEffect hook watching selectedTournament
    setPlayerStats([]); // Clear stats
  };

  const headers = useMemo(() => {
    const baseHeaders = [
      { label: "Player", sortKey: "player_name", type: "string" },
      { label: "Innings", sortKey: "total_innings_for_criteria", type: "number" }
    ];
    const dynamicHeaders = thresholds.split(',').map(th => {
      const thresholdValue = parseInt(th.trim(), 10);
      return { label: `${thresholdValue}+ ${getStatTypeLabel(activeStatsView)}`, sortKey: thresholdValue, type: "percentage" };
    });
    return [...baseHeaders, ...dynamicHeaders, { label: "Gamelog", sortKey: null, type: "action" }]; // Gamelog not sortable
  }, [thresholds, activeStatsView]);

  const sortedPlayerStats = useMemo(() => {
    let sortableItems = [...playerStats];
    if (sortConfig.key !== null) {
      sortableItems.sort((a, b) => {
        let aValue: any;
        let bValue: any;

        if (typeof sortConfig.key === 'string') {
          if (sortConfig.key === 'player_name') {
            aValue = a.player_name;
            bValue = b.player_name;
          } else if (sortConfig.key === 'total_innings_for_criteria') {
            // Assuming the first stat item has the representative total_innings_for_criteria
            aValue = a.stats.length > 0 ? a.stats[0].total_innings_for_criteria : 0;
            bValue = b.stats.length > 0 ? b.stats[0].total_innings_for_criteria : 0;
          }
        } else if (typeof sortConfig.key === 'number') { // Threshold columns
          const aStat = a.stats.find(s => s.threshold === sortConfig.key);
          const bStat = b.stats.find(s => s.threshold === sortConfig.key);
          aValue = aStat ? aStat.hit_rate_percentage : -1; // Use -1 to sort undefined/null to the bottom for ascending
          bValue = bStat ? bStat.hit_rate_percentage : -1;
        }

        if (aValue < bValue) {
          return sortConfig.direction === 'ascending' ? -1 : 1;
        }
        if (aValue > bValue) {
          return sortConfig.direction === 'ascending' ? 1 : -1;
        }
        return 0;
      });
    }
    return sortableItems;
  }, [playerStats, sortConfig]);

  // Custom select component for futuristic look
  const CustomSelect = ({ id, value, onChange, disabled, children, loading, defaultOptionText }: any) => (
    <div className="relative">
      <select
        id={id}
        value={value}
        onChange={onChange}
        className="appearance-none w-full p-3 bg-neutral-800 border border-neutral-700 rounded-md text-neutral-100 focus:ring-2 focus:ring-primary focus:border-primary transition-shadow duration-150 shadow-sm hover:border-neutral-600 disabled:opacity-50 disabled:cursor-not-allowed"
        disabled={disabled || loading}
      >
        <option value="">{loading ? "Loading..." : defaultOptionText}</option>
        {children}
      </select>
      <div className="pointer-events-none absolute inset-y-0 right-0 flex items-center px-2 text-neutral-400">
        <ChevronDown size={20} />
      </div>
    </div>
  );
  
  const CustomInput = ({ id, value, onChange, placeholder, min, type="text" }: any) => (
    <input
        type={type}
        id={id}
        value={value}
        onChange={onChange}
        placeholder={placeholder}
        min={min}
        className="w-full p-3 bg-neutral-800 border border-neutral-700 rounded-md text-neutral-100 focus:ring-2 focus:ring-primary focus:border-primary transition-shadow duration-150 shadow-sm hover:border-neutral-600 placeholder-neutral-500 disabled:opacity-50"
    />
  );

  const requestSort = (key: string | number | null) => {
    if (!key) return; // Not a sortable column
    let direction: 'ascending' | 'descending' = 'ascending';
    if (sortConfig.key === key && sortConfig.direction === 'ascending') {
      direction = 'descending';
    }
    setSortConfig({ key, direction });
  };

  return (
    <div className="container mx-auto p-4 md:p-6 text-neutral-100">
      <header className="mb-10 text-center">
        <h1 className="text-4xl md:text-5xl font-bold text-primary mb-3">Player Performance Analyzer</h1>
        <p className="text-lg text-neutral-400">
          Analyze T20 player statistics for runs, boundaries, and wickets.
        </p>
      </header>

      {/* Stat Type Selection Tabs - Futuristic Style */}
      <div className="mb-8 flex justify-center">
        <div className="flex space-x-1 p-1 bg-neutral-800 rounded-lg shadow-md">
            {(['runs', '4s', '6s', 'wickets'] as const).map((view) => (
              <button
                key={view}
                onClick={() => handleStatViewChange(view)}
                className={`px-5 py-2.5 rounded-md font-medium text-sm transition-all duration-200 ease-in-out
                            ${activeStatsView === view
                              ? 'bg-primary text-black shadow-lg' 
                              : 'text-neutral-400 hover:bg-neutral-700 hover:text-neutral-100'}`}
              >
                {view === 'runs' && <TrendingUp size={16} className="inline-block mr-1.5" />}
                {view === '4s' && <Zap size={16} className="inline-block mr-1.5" />}
                {view === '6s' && <BarChart3 size={16} className="inline-block mr-1.5" />}
                {view === 'wickets' && <Crosshair size={16} className="inline-block mr-1.5" />}
                {getStatTypeLabel(view)}
              </button>
            ))}
        </div>
      </div>
      
      {/* Filters Section - Futuristic Card Style */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-3 gap-5 mb-10 p-6 bg-neutral-900 shadow-xl rounded-xl border border-neutral-800">
        
        <div className="flex flex-col space-y-4 md:col-span-1">
            <div>
                <label htmlFor="tournament-type-select" className="block mb-1.5 text-xs font-semibold text-neutral-400 tracking-wide uppercase">Category</label>
                <CustomSelect id="tournament-type-select" value={selectedTournamentType} onChange={handleTournamentTypeChange} disabled={loadingTournaments} loading={loadingTournaments} defaultOptionText="All Categories">
                    <option value="all">All Categories</option>
                    <option value="Franchise">Franchise Leagues</option>
                    <option value="International">International T20s</option>
                </CustomSelect>
            </div>
            <div>
                <label htmlFor="tournament-select" className="block mb-1.5 text-xs font-semibold text-neutral-400 tracking-wide uppercase">Tournament</label>
                <CustomSelect id="tournament-select" value={selectedTournament} onChange={handleTournamentChange} disabled={loadingTournaments || tournaments.length === 0} loading={loadingTournaments} defaultOptionText={selectedTournamentType !== 'all' && tournaments.length === 0 ? "No tournaments for category" : "All Tournaments in Category"}>
                    {tournaments.map(t => <option key={t.name} value={t.name}>{t.name}</option>)}
                </CustomSelect>
            </div>
        </div>

        <div className="flex flex-col space-y-4 md:col-span-1">
            <div>
                <label htmlFor="team-select" className="block mb-1.5 text-xs font-semibold text-neutral-400 tracking-wide uppercase">Team</label>
                <CustomSelect id="team-select" value={selectedTeam} onChange={(e:any) => setSelectedTeam(e.target.value)} disabled={loadingTeams || ((!selectedTournament && selectedTournamentType === 'all'))} loading={loadingTeams} defaultOptionText={(teams.length === 0 && (selectedTournament || selectedTournamentType !== 'all')) ? "No teams available" : "All Teams"}>
                    {teams.map(t => <option key={t.name} value={t.name}>{t.name}</option>)}
                </CustomSelect>
            </div>
            <div>
                <label htmlFor="opposition-select" className="block mb-1.5 text-xs font-semibold text-neutral-400 tracking-wide uppercase">Opposition</label>
                <CustomSelect id="opposition-select" value={selectedOpposition} onChange={(e:any) => setSelectedOpposition(e.target.value)} disabled={loadingTeams || ((!selectedTournament && selectedTournamentType === 'all'))} loading={loadingTeams} defaultOptionText={(teams.length === 0 && (selectedTournament || selectedTournamentType !== 'all')) ? "No teams available" : "Any Opposition"}>
                    {teams.map(t => <option key={t.name} value={t.name}>{t.name}</option>)}
                </CustomSelect>
            </div>
        </div>

        <div className="flex flex-col space-y-4 md:col-span-1">
            <div>
                <label htmlFor="venue-select" className="block mb-1.5 text-xs font-semibold text-neutral-400 tracking-wide uppercase">Venue</label>
                <CustomSelect id="venue-select" value={selectedVenue} onChange={(e:any) => setSelectedVenue(e.target.value)} disabled={loadingVenues || ((!selectedTournament && selectedTournamentType === 'all'))} loading={loadingVenues} defaultOptionText={(venuesList.length === 0 && (selectedTournament || selectedTournamentType !== 'all')) ? "No venues available" : "Any Venue"}>
                    {venuesList.map(v => <option key={v.name} value={v.name}>{v.name}</option>)}
                </CustomSelect>
            </div>
            <div>
                <label htmlFor="last-n-seasons" className="block mb-1.5 text-xs font-semibold text-neutral-400 tracking-wide uppercase">Last N Seasons</label>
                <CustomInput type="number" id="last-n-seasons" value={lastNSeasons} onChange={(e:any) => setLastNSeasons(e.target.value)} placeholder="e.g., 3" min="1" />
            </div>
        </div>

        <div className="md:col-span-2 lg:col-span-3 xl:col-span-3 grid grid-cols-1 md:grid-cols-2 gap-5 items-end pt-4 border-t border-neutral-800 mt-2">
            <div className="md:col-span-1">
                <label htmlFor="min-player-innings" className="block mb-1.5 text-xs font-semibold text-neutral-400 tracking-wide uppercase">Min Player Innings</label>
                <CustomInput type="number" id="min-player-innings" value={minPlayerInnings} onChange={(e:any) => setMinPlayerInnings(e.target.value)} placeholder="e.g., 5" min="1" />
            </div>
            <div className="md:col-span-1">
                <label htmlFor="thresholds-input" className="block mb-1.5 text-xs font-semibold text-neutral-400 tracking-wide uppercase">Thresholds ({getStatTypeLabel(activeStatsView)})</label>
                <CustomInput id="thresholds-input" value={thresholds} onChange={(e:any) => setThresholds(e.target.value)} placeholder="e.g., 10,20,30" />
            </div>
        </div>
        
        <div className="md:col-span-3 flex justify-end items-end pt-4 mt-2">
          <button
            onClick={handleFetchStats}
            className="w-full md:w-auto p-3 bg-primary text-black rounded-lg hover:bg-primary/80 transition-all duration-200 flex items-center justify-center font-bold text-sm shadow-lg hover:shadow-primary/40 disabled:opacity-60 disabled:cursor-not-allowed focus:outline-none focus:ring-2 focus:ring-primary focus:ring-offset-2 focus:ring-offset-neutral-900"
            disabled={loadingStats || (!selectedTournament && selectedTournamentType === 'all' && !selectedTeam)}
          >
            <Search size={18} className="mr-2" />
            Fetch Player Stats
          </button>
        </div>
      </div>

      {error && <div className="my-6 p-4 bg-red-900/50 border border-red-700 text-red-300 rounded-lg shadow-md text-center">Error: {error}</div>}

      {loadingStats && (
        <div className="flex justify-center items-center py-12">
          <Loader2 className="h-12 w-12 animate-spin text-primary" />
          <p className="ml-3 text-lg text-neutral-400">Loading Stats...</p>
        </div>
      )}

      {!loadingStats && playerStats.length > 0 && (
        <div className="overflow-x-auto bg-neutral-900 shadow-xl rounded-lg border border-neutral-800">
          <table className="min-w-full divide-y divide-neutral-800">
            <thead className="bg-neutral-800/50">
              <tr>
                {headers.map(header => (
                  <th key={header.label} scope="col" className="px-5 py-3.5 text-left text-xs font-semibold text-neutral-400 uppercase tracking-wider whitespace-nowrap cursor-pointer group" onClick={() => requestSort(header.sortKey)}>
                    {header.label}
                    {header.sortKey && (
                      <span className="ml-2 inline-block opacity-0 group-hover:opacity-100 transition-opacity">
                        {sortConfig.key === header.sortKey ? (
                          sortConfig.direction === 'ascending' ? <ChevronUp size={14} /> : <ChevronDown size={14} />
                        ) : (
                          <ChevronsUpDown size={14} className="text-neutral-500" />
                        )}
                      </span>
                    )}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-neutral-800">
              {sortedPlayerStats.map((player) => (
                <React.Fragment key={player.player_name}>
                  <tr className="hover:bg-neutral-800/60 transition-colors duration-150">
                    <td className="px-5 py-4 whitespace-nowrap text-sm font-medium text-neutral-100">{player.player_name}</td>
                    <td className="px-5 py-4 whitespace-nowrap text-sm text-neutral-300">
                      {player.stats.length > 0 ? player.stats[0].total_innings_for_criteria : '-'}
                    </td>
                    {thresholds.split(',').map(th => {
                      const stat = player.stats.find(s => s.threshold === parseInt(th.trim(), 10));
                      const hitRate = stat ? stat.hit_rate_percentage : null;
                      const success = stat ? stat.innings_met_or_exceeded_threshold : 0;
                      const total = stat ? stat.total_innings_for_criteria : 0;
                      let textColor = 'text-neutral-300';
                      if (hitRate !== null) {
                        if (hitRate >= 75) textColor = 'text-green-400';
                        else if (hitRate >= 50) textColor = 'text-yellow-400';
                        else if (hitRate > 0) textColor = 'text-orange-400';
                        else if (hitRate === 0 && total > 0) textColor = 'text-red-500';
                      }
                       return (
                        <td key={th} className={`px-5 py-4 whitespace-nowrap text-sm text-center font-medium ${textColor}`}>
                           {stat ? `${hitRate}% (${success}/${total})` : <span className="text-neutral-600">-</span>}
                        </td>
                      );
                    })}
                    <td className="px-5 py-4 whitespace-nowrap text-sm text-center">
                      {player.gamelog && player.gamelog.length > 0 ? (
                        <button onClick={() => toggleGamelog(player.player_name)} className="text-primary hover:text-primary/70 p-1 rounded-md focus:outline-none focus:ring-2 focus:ring-primary/50">
                          {expandedGamelogs[player.player_name] ? <EyeOff size={18} /> : <Eye size={18} />}
                        </button>
                      ) : (
                        <span className="text-neutral-600">-</span>
                      )}
                    </td>
                  </tr>
                  {expandedGamelogs[player.player_name] && player.gamelog && player.gamelog.length > 0 && (
                    <tr>
                      <td colSpan={headers.length} className="p-0">
                        <div className="bg-neutral-800/30 p-5 max-h-[28rem] overflow-y-auto border-t border-b border-neutral-700">
                          <h4 className="text-base font-semibold mb-3 text-neutral-100">Gamelog: {player.player_name} ({getStatTypeLabel(activeStatsView)})</h4>
                          <div className="overflow-x-auto">
                            <table className="min-w-full divide-y divide-neutral-700 text-xs">
                              <thead className="bg-neutral-700/50">
                                <tr>
                                  {activeStatsView === 'wickets' ? 
                                    ["Date", "Bowling For", "Vs", "Venue (City)", "Wkts", "Balls", "Runs Conceded", "Econ"].map(gh => (
                                      <th key={gh} className={`px-3 py-2.5 text-left font-medium text-neutral-400 tracking-wider ${gh === "Wkts" || gh === "Balls" || gh === "Runs Conceded" || gh === "Econ" ? "text-right" : ""}`}>{gh}</th>
                                    ))
                                    :
                                    ["Date", "Bat Team", "Vs", "Venue (City)", activeStatsView === 'runs' ? 'Runs' : activeStatsView === '4s' ? '4s' : '6s', "BF", "SR", "Dismissal"].map(gh => (
                                      <th key={gh} className={`px-3 py-2.5 text-left font-medium text-neutral-400 tracking-wider ${gh === "Runs" || gh === "4s" || gh === "6s" || gh === "BF" || gh === "SR" ? "text-right" : ""}`}>{gh}</th>
                                    ))
                                  }
                                </tr>
                              </thead>
                              <tbody className="divide-y divide-neutral-700">
                                {player.gamelog.map(log => (
                                  <tr key={log.match_id + log.innings_no} className="hover:bg-neutral-700/40 transition-colors duration-100">
                                    {isBowlerGamelogEntry(log) && activeStatsView === 'wickets' ? (
                                      <>
                                        <td className="px-3 py-2.5 whitespace-nowrap text-neutral-300">{new Date(log.match_date).toLocaleDateString()}</td>
                                        <td className="px-3 py-2.5 whitespace-nowrap text-neutral-300">{log.bowling_team_for_player}</td>
                                        <td className="px-3 py-2.5 whitespace-nowrap text-neutral-300">{log.batting_team_opponent}</td>
                                        <td className="px-3 py-2.5 whitespace-nowrap text-neutral-300">{log.city || 'N/A'}</td>
                                        <td className="px-3 py-2.5 whitespace-nowrap text-right font-semibold text-neutral-100">{log.wickets_taken_in_inning}</td>
                                        <td className="px-3 py-2.5 whitespace-nowrap text-right text-neutral-300">{log.balls_bowled_in_inning}</td>
                                        <td className="px-3 py-2.5 whitespace-nowrap text-right text-neutral-300">{log.runs_conceded_in_inning}</td>
                                        <td className="px-3 py-2.5 whitespace-nowrap text-right text-neutral-300">{log.economy_rate_in_inning?.toFixed(2) || '-'}</td>
                                      </>
                                    ) : !isBowlerGamelogEntry(log) && (activeStatsView === 'runs' || activeStatsView === '4s' || activeStatsView === '6s') ? (
                                      <>
                                        <td className="px-3 py-2.5 whitespace-nowrap text-neutral-300">{new Date(log.match_date).toLocaleDateString()}</td>
                                        <td className="px-3 py-2.5 whitespace-nowrap text-neutral-300">{log.batting_team}</td>
                                        <td className="px-3 py-2.5 whitespace-nowrap text-neutral-300">{log.bowling_team}</td>
                                        <td className="px-3 py-2.5 whitespace-nowrap text-neutral-300">{log.city || 'N/A'}</td>
                                        <td className="px-3 py-2.5 whitespace-nowrap text-right font-semibold text-neutral-100">
                                            {activeStatsView === 'runs' ? log.runs_scored :
                                             activeStatsView === '4s' ? log.fours :
                                             log.sixes}
                                        </td>
                                        <td className="px-3 py-2.5 whitespace-nowrap text-right text-neutral-300">{log.balls_faced}</td>
                                        <td className="px-3 py-2.5 whitespace-nowrap text-right text-neutral-300">{log.strike_rate !== null && log.strike_rate !== undefined ? log.strike_rate.toFixed(1) : '-'}</td>
                                        <td className="px-3 py-2.5 whitespace-nowrap text-neutral-400 text-xs">{log.wicket_type ? `${log.wicket_type} (b ${log.wicket_bowler || '-'})` : <span className="text-green-400">Not Out</span>}</td>
                                      </>
                                    ) : null}
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        </div>
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              ))}
            </tbody>
          </table>
          </div>
      )}
      {!loadingStats && playerStats.length === 0 && !error && (
        <div className="text-center py-12 text-neutral-500">
          <Filter size={40} className="mx-auto mb-5 opacity-40" />
          <p className="text-lg mb-1">No Player Stats to Display</p>
          <p className="text-sm">Please adjust your filters and click "Fetch Player Stats". If filters are set, no players matched your criteria.</p>
        </div>
      )}
    </div>
  );
};

// New default export for the page that wraps the content
const PropsAnalyzerPageWithSuspense: React.FC = () => {
  return (
    <Suspense fallback={<div>Loading page content...</div>}> {/* Or a more sophisticated Skeleton loader */}
      <PropsAnalyzerContent />
    </Suspense>
  );
};

// Default export for Vercel to pick up
export default PropsAnalyzerPageWithSuspense; 