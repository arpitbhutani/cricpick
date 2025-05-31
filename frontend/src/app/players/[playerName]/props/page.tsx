'use client';

import { useEffect, useState, useCallback } from 'react';
import { useParams } from 'next/navigation';
import { BarChart2, Search, Filter, Loader2, AlertTriangle, Target, TrendingUp, TrendingDown, ListChecks, Eye, EyeOff, ShieldCheck, CalendarDays, MapPin, Users, XCircle, Globe, Building } from 'lucide-react';

// --- Data Interfaces (mirroring backend Pydantic models) ---
interface ThresholdStat {
  threshold: number;
  total_innings_for_criteria: number;
  innings_met_or_exceeded_threshold: number;
  hit_rate_percentage: number;
}

// --- Gamelog Entry Interface (mirroring backend Pydantic model) ---
interface PlayerGamelogEntry {
  match_id: string;
  match_date: string; // Dates are typically strings in JSON
  batting_team: string;
  bowling_team: string;
  city?: string | null;
  innings_no: number;
  runs_scored: number;
  balls_faced: number;
  fours: number;
  sixes: number;
  wicket_type?: string | null;
  wicket_bowler?: string | null;
  wicket_fielders?: string | null;
  strike_rate: number | null;
}
// --- End Gamelog Entry Interface ---

interface BasePlayerStats {
  player_name: string;
  stats: ThresholdStat[];
  gamelog?: PlayerGamelogEntry[]; 
}

interface BatterRunsPlayerStats extends BasePlayerStats {}
interface BatterBoundaryPlayerStats extends BasePlayerStats {
  boundary_type: '4s' | '6s';
}

// --- Filter Option Interfaces (mirroring backend Pydantic models) ---
interface Tournament {
  name: string;
  type: string;
}

interface Season {
  season: string;
}

interface Venue {
  name: string;
}

interface Team {
  name: string;
}

interface PlayerFilterOptions {
  tournaments: Tournament[];
  seasons: Season[];
  venues: Venue[];
  oppositionTeams: Team[];
}
// --- End Filter Option Interfaces ---

// --- Component Props (for URL params) ---
interface PlayerPropsPageParams {
  playerName: string;
}

export default function PlayerPropsPage() {
  const params = useParams();
  const [playerName, setPlayerName] = useState<string>('');

  // Stats states
  const [runsStats, setRunsStats] = useState<BatterRunsPlayerStats | null>(null);
  const [foursStats, setFoursStats] = useState<BatterBoundaryPlayerStats | null>(null);
  const [sixesStats, setSixesStats] = useState<BatterBoundaryPlayerStats | null>(null);
  const [customRunsStats, setCustomRunsStats] = useState<BatterRunsPlayerStats | null>(null); // For custom threshold stats
  const [isLoadingCustomRuns, setIsLoadingCustomRuns] = useState<boolean>(false);

  // Filter states
  const [selectedTournaments, setSelectedTournaments] = useState<string[]>([]);
  const [selectedSeasons, setSelectedSeasons] = useState<string[]>([]); // Could be single selection if API supports only one
  const [selectedVenues, setSelectedVenues] = useState<string[]>([]);
  const [selectedOppositionTeams, setSelectedOppositionTeams] = useState<string[]>([]);
  const [minInnings, setMinInnings] = useState<number | string>(1); // string for input field
  const [includeGamelog, setIncludeGamelog] = useState<boolean>(false);
  const [defaultRunThresholds, setDefaultRunThresholds] = useState("10,20,30,40,50");
  const [defaultBoundaryThresholds, setDefaultBoundaryThresholds] = useState("1,2,3,4,5");
  const [tournamentTypeFilter, setTournamentTypeFilter] = useState<'all' | 'Franchise' | 'International'>('all');

  // Filter options from player-specific endpoint
  const [playerFilterOptions, setPlayerFilterOptions] = useState<PlayerFilterOptions | null>(null);
  const [displayedTournaments, setDisplayedTournaments] = useState<Tournament[]>([]);

  // UI states
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<'runs' | 'fours' | 'sixes'>('runs');

  // Custom Run Range - User can input a comma-separated string for thresholds
  const [customRunThresholds, setCustomRunThresholds] = useState<string>('');
  const [appliedCustomRunThresholds, setAppliedCustomRunThresholds] = useState<string>(''); // To store the last applied custom thresholds

  useEffect(() => {
    if (params?.playerName) {
      setPlayerName(decodeURIComponent(params.playerName as string));
      // Reset filters when player changes
      setSelectedTournaments([]);
      setSelectedSeasons([]);
      setSelectedVenues([]);
      setSelectedOppositionTeams([]);
      setMinInnings(1);
      setIncludeGamelog(false);
      setTournamentTypeFilter('all'); // Ensure reset to 'all'
      setPlayerFilterOptions(null); // Clear old options
      setDisplayedTournaments([]);
      setRunsStats(null);
      setFoursStats(null);
      setSixesStats(null);
      setCustomRunsStats(null); // Clear custom stats
      setCustomRunThresholds(''); // Clear input field
      setAppliedCustomRunThresholds(''); // Clear applied custom thresholds
    }
  }, [params]);

  // --- Data Fetching Logic ---
  const fetchPlayerPropsData = useCallback(async () => {
    if (!playerName) return;

    setIsLoading(true);
    setError(null);

    // Construct query params string from filters
    const queryParams = new URLSearchParams();

    // Tournament filtering logic based on tournamentTypeFilter and selectedTournaments
    if (selectedTournaments.length > 0) {
      queryParams.append('tournament', selectedTournaments.join(','));
    } else if (tournamentTypeFilter !== 'all' && playerFilterOptions?.tournaments) {
      const tournamentsOfType = playerFilterOptions.tournaments
        .filter(t => t.type === tournamentTypeFilter)
        .map(t => t.name);
      if (tournamentsOfType.length > 0) {
        queryParams.append('tournament', tournamentsOfType.join(','));
      } else {
        // If player has no tournaments of this type, we can send a query that will return no results
        // For example, by sending a non-existent tournament name.
        // This ensures the API call is made but yields empty data as expected.
        queryParams.append('tournament', '__NO_TOURNAMENTS_OF_SELECTED_TYPE__');
      }
    }

    // For now, let's assume API handles single season, or we adapt later. For now, join if multiple are somehow selected.
    if (selectedSeasons.length > 0) queryParams.append('season', selectedSeasons.join(',')); 
    if (selectedVenues.length > 0) queryParams.append('venue', selectedVenues.join(','));
    if (selectedOppositionTeams.length > 0) queryParams.append('opposition', selectedOppositionTeams.join(','));
    if (minInnings !== '' && Number(minInnings) >= 0) queryParams.append('min_innings', String(minInnings));
    if (includeGamelog) queryParams.append('include_gamelog', 'true');

    try {
      // Fetch Runs Stats
      const runsResponse = await fetch(`/api/batters/stats/runs_hit_rates?players=${encodeURIComponent(playerName)}&thresholds=${defaultRunThresholds}&${queryParams.toString()}`);
      if (!runsResponse.ok) throw new Error(`Failed to fetch runs stats: ${runsResponse.status} ${await runsResponse.text()}`);
      const runsData: BatterRunsPlayerStats[] = await runsResponse.json();
      setRunsStats(runsData.length > 0 ? runsData[0] : { player_name: playerName, stats: [], gamelog: [] });

      // Fetch 4s Stats
      const foursResponse = await fetch(`/api/batters/stats/boundary_hit_rates?players=${encodeURIComponent(playerName)}&boundary_type=4s&thresholds=${defaultBoundaryThresholds}&${queryParams.toString()}`);
      if (!foursResponse.ok) throw new Error(`Failed to fetch 4s stats: ${foursResponse.status} ${await foursResponse.text()}`);
      const foursData: BatterBoundaryPlayerStats[] = await foursResponse.json();
      setFoursStats(foursData.length > 0 ? foursData[0] : { player_name: playerName, boundary_type: '4s', stats: [], gamelog: [] });

      // Fetch 6s Stats
      const sixesResponse = await fetch(`/api/batters/stats/boundary_hit_rates?players=${encodeURIComponent(playerName)}&boundary_type=6s&thresholds=${defaultBoundaryThresholds}&${queryParams.toString()}`);
      if (!sixesResponse.ok) throw new Error(`Failed to fetch 6s stats: ${sixesResponse.status} ${await sixesResponse.text()}`);
      const sixesData: BatterBoundaryPlayerStats[] = await sixesResponse.json();
      setSixesStats(sixesData.length > 0 ? sixesData[0] : { player_name: playerName, boundary_type: '6s', stats: [], gamelog: [] });

    } catch (err) {
      console.error("Error fetching player prop data:", err);
      setError(err instanceof Error ? err.message : 'An unknown error occurred while fetching prop data.');
      setRunsStats(null); // Clear data on error
      setFoursStats(null);
      setSixesStats(null);
    }
    setIsLoading(false);
  }, [
    playerName, 
    selectedTournaments, 
    selectedSeasons, 
    selectedVenues, 
    selectedOppositionTeams, 
    minInnings, 
    includeGamelog,
    defaultRunThresholds,
    defaultBoundaryThresholds,
    tournamentTypeFilter,
    playerFilterOptions
  ]);

  const fetchFilterOptions = useCallback(async () => {
    if (!playerName) return;
    try {
      // Fetch player-specific filter options
      const response = await fetch(`/api/players/${encodeURIComponent(playerName)}/filter-options`);
      if (!response.ok) {
        throw new Error(`Failed to fetch filter options for ${playerName}: ${response.status} ${await response.text()}`);
      }
      const options: PlayerFilterOptions = await response.json();
      setPlayerFilterOptions(options);
      // Initially display all tournaments for the player
      setDisplayedTournaments(options.tournaments || []); 
    } catch (err) {
      console.error("Error fetching filter options:", err);
      setError(err instanceof Error ? err.message : "Could not load filter options. Some filters may not work.");
      setPlayerFilterOptions(null);
    }
  }, [playerName]);

  useEffect(() => {
    fetchFilterOptions();
  }, [fetchFilterOptions]);

  // Effect to update displayedTournaments when tournamentTypeFilter or playerFilterOptions change
  useEffect(() => {
    if (playerFilterOptions?.tournaments) {
      if (tournamentTypeFilter === 'all') {
        setDisplayedTournaments(playerFilterOptions.tournaments);
      } else {
        setDisplayedTournaments(playerFilterOptions.tournaments.filter(t => t.type === tournamentTypeFilter));
      }
    } else {
      setDisplayedTournaments([]); // Clear if no filter options available
    }
  }, [tournamentTypeFilter, playerFilterOptions]);

  useEffect(() => {
    if (playerName) {
      fetchPlayerPropsData();
    }
  }, [playerName, fetchPlayerPropsData]);

  const handleClearFilters = () => {
    setSelectedTournaments([]);
    setSelectedSeasons([]);
    setSelectedVenues([]);
    setSelectedOppositionTeams([]);
    setMinInnings(1);
    setTournamentTypeFilter('all'); // Reset to 'all'
    setCustomRunsStats(null); // Clear custom run stats on filter clear
    setCustomRunThresholds(''); // Optionally clear the input field too
    setAppliedCustomRunThresholds('');
    // setIncludeGamelog(false); // Optional: decide if gamelog toggle should also reset
    // fetchPlayerPropsData(); // Manually trigger if Apply Filters button isn't the only way
  };

  const handleApplyCustomRunThresholds = async () => {
    if (!playerName || customRunThresholds.trim() === '') {
      // Clear custom stats if input is empty, or do nothing
      setCustomRunsStats(null);
      setAppliedCustomRunThresholds('');
      alert("Custom run thresholds input is empty. Custom stats cleared if any.");
      return;
    }

    // Validate (simple validation for comma-separated numbers, allows leading/trailing spaces around numbers)
    const isValid = /^\s*\d+\s*(,\s*\d+\s*)*$/.test(customRunThresholds.trim());
    if (!isValid) {
      alert("Invalid custom run thresholds. Please use comma-separated numbers (e.g., 15,30,45).");
      return;
    }
    
    const normalizedThresholds = customRunThresholds.split(',').map(s => s.trim()).filter(s => s !== '').join(',');
    if (!normalizedThresholds) {
        alert("Custom run thresholds cannot be empty after normalization.");
        return;
    }

    setIsLoadingCustomRuns(true);
    setError(null); // Clear previous errors for this specific action

    const queryParams = new URLSearchParams();
    // Apply current filters to custom stats fetch
    if (selectedTournaments.length > 0) {
      queryParams.append('tournament', selectedTournaments.join(','));
    } else if (tournamentTypeFilter !== 'all' && playerFilterOptions?.tournaments) {
      const tournamentsOfType = playerFilterOptions.tournaments
        .filter(t => t.type === tournamentTypeFilter)
        .map(t => t.name);
      if (tournamentsOfType.length > 0) {
        queryParams.append('tournament', tournamentsOfType.join(','));
      } else {
        queryParams.append('tournament', '__NO_TOURNAMENTS_OF_SELECTED_TYPE__');
      }
    }
    if (selectedSeasons.length > 0) queryParams.append('season', selectedSeasons.join(','));
    if (selectedVenues.length > 0) queryParams.append('venue', selectedVenues.join(','));
    if (selectedOppositionTeams.length > 0) queryParams.append('opposition', selectedOppositionTeams.join(','));
    if (minInnings !== '' && Number(minInnings) >= 0) queryParams.append('min_innings', String(minInnings));
    // Gamelog for custom stats could be a separate toggle or follow main one
    // if (includeGamelog) queryParams.append('include_gamelog', 'true'); 

    try {
      const response = await fetch(`/api/batters/stats/runs_hit_rates?players=${encodeURIComponent(playerName)}&thresholds=${normalizedThresholds}&${queryParams.toString()}`);
      if (!response.ok) {
        throw new Error(`Failed to fetch custom runs stats: ${response.status} ${await response.text()}`);
      }
      const data: BatterRunsPlayerStats[] = await response.json();
      setCustomRunsStats(data.length > 0 ? data[0] : { player_name: playerName, stats: [] });
      setAppliedCustomRunThresholds(normalizedThresholds); // Store the successfully applied thresholds
    } catch (err) {
      console.error("Error fetching custom run stats:", err);
      setError(err instanceof Error ? err.message : 'An unknown error occurred while fetching custom prop data.');
      setCustomRunsStats(null);
      setAppliedCustomRunThresholds('');
    }
    setIsLoadingCustomRuns(false);
  };

  // Helper function to create a unique key for multi-select items
  const getSelectKey = (item: string | Tournament | Season | Venue | Team, index: number) => {
    if (typeof item === 'string') return `${item}-${index}`;
    if ('name' in item) return `${item.name}-${index}`;
    if ('season' in item) return `${(item as Season).season}-${index}`;
    return `key-${index}`;
  };
  
  const renderFilterSection = () => (
    <div className="bg-gray-800 p-6 rounded-lg shadow-xl mb-8">
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-4">
        {/* Tournament Type Filter */}
        <div>
          <label htmlFor="tournamentType" className="block text-sm font-medium text-gray-300 mb-1">
            <Globe className="inline-block mr-2 h-5 w-5 text-blue-400" />
            Tournament Type
          </label>
          <select
            id="tournamentType"
            value={tournamentTypeFilter}
            onChange={(e) => {
              setTournamentTypeFilter(e.target.value as 'all' | 'Franchise' | 'International');
              setSelectedTournaments([]); // Clear specific tournament selections when type changes
            }}
            className="w-full bg-gray-700 border-gray-600 text-white rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 p-2"
          >
            <option value="all">All Types</option>
            <option value="International">International</option>
            <option value="Franchise">Franchise</option>
            {/* <option value="Other">Other</option> */}
            {/* 'Other' type can be added if the backend distinguishes it and it's useful */}
          </select>
        </div>

        {/* Tournaments Multi-Select */}
        <div>
          <label htmlFor="tournaments" className="block text-sm font-medium text-gray-300 mb-1">
            <Building className="inline-block mr-2 h-5 w-5 text-green-400" />
            Tournaments
          </label>
          <select
            id="tournaments"
            multiple
            value={selectedTournaments}
            onChange={(e) => setSelectedTournaments(Array.from(e.target.selectedOptions, option => option.value))}
            className="w-full bg-gray-700 border-gray-600 text-white rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 p-2 h-32"
            disabled={!playerFilterOptions || displayedTournaments.length === 0}
          >
            {displayedTournaments.map((tournament, index) => (
              <option key={getSelectKey(tournament, index)} value={tournament.name}>
                {tournament.name} ({tournament.type})
              </option>
            ))}
          </select>
          {displayedTournaments.length === 0 && playerFilterOptions && <p className="text-xs text-gray-400 mt-1">No {tournamentTypeFilter !== 'all' ? tournamentTypeFilter : ''} tournaments found for this player.</p>}
        </div>

        {/* Seasons Multi-Select */}
        <div>
          <label htmlFor="seasons" className="block text-sm font-medium text-gray-300 mb-1">
            <CalendarDays className="inline-block mr-2 h-5 w-5 text-purple-400" />
            Seasons
          </label>
          <select
            id="seasons"
            multiple
            value={selectedSeasons}
            onChange={(e) => setSelectedSeasons(Array.from(e.target.selectedOptions, option => option.value))}
            className="w-full bg-gray-700 border-gray-600 text-white rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 p-2 h-32"
            disabled={!playerFilterOptions || !playerFilterOptions.seasons || playerFilterOptions.seasons.length === 0}
          >
            {playerFilterOptions?.seasons && playerFilterOptions.seasons.length > 0 ? (
              playerFilterOptions.seasons.map((season, index) => (
                <option key={getSelectKey(season, index)} value={season.season}>
                  {season.season}
                </option>
              ))
            ) : (
              <option disabled>No seasons played.</option>
            )}
          </select>
        </div>

        {/* Venues Multi-Select */}
        <div>
          <label htmlFor="venues" className="block text-sm font-medium text-gray-300 mb-1">
            <MapPin className="inline-block mr-2 h-5 w-5 text-pink-400" />
            Venues
          </label>
          <select
            id="venues"
            multiple
            value={selectedVenues}
            onChange={(e) => setSelectedVenues(Array.from(e.target.selectedOptions, option => option.value))}
            className="w-full bg-gray-700 border-gray-600 text-white rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 p-2 h-32"
            disabled={!playerFilterOptions || !playerFilterOptions.venues || playerFilterOptions.venues.length === 0}
          >
            {playerFilterOptions?.venues && playerFilterOptions.venues.length > 0 ? (
              playerFilterOptions.venues.map((venue, index) => (
                <option key={getSelectKey(venue, index)} value={venue.name}>
                  {venue.name}
                </option>
              ))
            ) : (
              <option disabled>No venues played at.</option>
            )}
          </select>
        </div>

        {/* Opposition Teams Multi-Select */}
        <div>
          <label htmlFor="opposition" className="block text-sm font-medium text-gray-300 mb-1">
            <Users className="inline-block mr-2 h-5 w-5 text-orange-400" />
            Opposition Teams
          </label>
          <select
            id="opposition"
            multiple
            value={selectedOppositionTeams}
            onChange={(e) => setSelectedOppositionTeams(Array.from(e.target.selectedOptions, option => option.value))}
            className="w-full bg-gray-700 border-gray-600 text-white rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 p-2 h-32"
            disabled={!playerFilterOptions || !playerFilterOptions.oppositionTeams || playerFilterOptions.oppositionTeams.length === 0}
          >
            {playerFilterOptions?.oppositionTeams && playerFilterOptions.oppositionTeams.length > 0 ? (
              playerFilterOptions.oppositionTeams.map((team, index) => (
                <option key={getSelectKey(team, index)} value={team.name}>
                  {team.name}
                </option>
              ))
            ) : (
              <option disabled>No opposition faced.</option>
            )}
          </select>
        </div>

        {/* Min. Innings Input */}
        <div>
          <label htmlFor="minInnings" className="block text-sm font-medium text-gray-300 mb-1">
            <ListChecks className="inline-block mr-2 h-5 w-5 text-teal-400" />
            Min. Innings
          </label>
          <input
            type="number"
            id="minInnings"
            value={minInnings}
            onChange={(e) => setMinInnings(e.target.value ? Math.max(0, parseInt(e.target.value)) : '')}
            className="w-full bg-gray-700 border-gray-600 text-white rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 p-2"
            placeholder="e.g., 5"
          />
        </div>
      </div>

      {/* Action Buttons: Apply Filters, Clear Filters, Toggle Gamelog */}
      <div className="flex items-center space-x-4">
        <button
          onClick={() => fetchPlayerPropsData()}
          className="flex-grow bg-blue-500 hover:bg-blue-600 text-white font-semibold py-2 px-4 rounded-md shadow-md transition duration-150 ease-in-out flex items-center"
          disabled={isLoading}
        >
          {isLoading ? <Loader2 className="animate-spin h-5 w-5 mr-2" /> : <Search className="mr-2 h-5 w-5" />} Apply All Filters
        </button>
        <button
          onClick={handleClearFilters}
          title="Clear all filters"
          className="p-2 bg-gray-700 hover:bg-gray-600 text-white rounded-md shadow-md transition duration-150 ease-in-out flex items-center"
          disabled={isLoading}
        >
          <XCircle className="mr-2 h-5 w-5" /> Clear Filters
        </button>
        <div className="flex items-center space-x-2">
          <input
            type="checkbox"
            id="includeGamelog"
            checked={includeGamelog}
            onChange={(e) => setIncludeGamelog(e.target.checked)}
            className="form-checkbox h-5 w-5 text-blue-500"
          />
          <label htmlFor="includeGamelog" className="text-sm leading-5 font-medium text-gray-300">
            Include Gamelog
          </label>
        </div>
      </div>
    </div>
  );

  if (!playerName) {
    return (
      <div className="flex flex-col items-center justify-center min-h-[calc(100vh-200px)]">
        <Loader2 className="h-12 w-12 animate-spin text-accent mb-4" />
        <p className="text-xl text-textSecondary">Loading player details...</p>
      </div>
    );
  }
  
  // --- Render Logic ---
  return (
    <div className="container mx-auto px-2 py-6 md:px-4 md:py-8">
      <div className="bg-card p-4 md:p-6 rounded-xl shadow-2xl">
        <div className="flex flex-col md:flex-row md:items-center md:justify-between mb-6 pb-4 border-b border-border">
            <h1 className="text-3xl md:text-4xl font-bold text-primary mb-2 md:mb-0">
              {playerName} - Prop Stats
            </h1>
            {/* Link back to player search or a general batters page could go here */}
        </div>

        {renderFilterSection()}

        {/* Custom Run Thresholds Input */}
        <div className="bg-gray-800 p-4 rounded-lg shadow-md mb-6">
          <h3 className="text-lg font-semibold text-gray-100 mb-3 flex items-center">
            <Target className="inline-block mr-2 h-5 w-5 text-yellow-400" />
            Custom Run Props (Overrides Default)
          </h3>
          <div className="flex items-center gap-4">
            <input
              type="text"
              placeholder="e.g., 15,30,45 (applies with current filters)"
              value={customRunThresholds}
              onChange={(e) => setCustomRunThresholds(e.target.value)}
              className="flex-grow bg-gray-700 border-gray-600 text-white rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 p-2"
            />
            <button
              onClick={handleApplyCustomRunThresholds}
              className="bg-yellow-500 hover:bg-yellow-600 text-black font-semibold py-2 px-4 rounded-md shadow-md transition duration-150 ease-in-out flex items-center whitespace-nowrap"
              disabled={isLoadingCustomRuns || !playerName}
            >
              {isLoadingCustomRuns ? <Loader2 className="animate-spin h-5 w-5 mr-2" /> : <TrendingUp className="mr-2 h-5 w-5" />}
              Apply Custom Runs
            </button>
          </div>
          <p className="text-xs text-gray-400 mt-2">
            Enter comma-separated run values. These will be fetched with the currently active filters and displayed as additional rows in the Runs table.
          </p>
        </div>

        {isLoading && (
          <div className="flex justify-center items-center min-h-[200px]">
            <Loader2 className="h-10 w-10 animate-spin text-accent" />
            <p className="ml-3 text-lg text-textSecondary">Loading prop stats for {playerName}...</p>
          </div>
        )}

        {error && (
          <div className="bg-red-100 border-l-4 border-red-500 text-red-700 p-4 rounded-md shadow my-6" role="alert">
            <div className="flex items-center">
              <AlertTriangle size={24} className="mr-3" />
              <p className="font-semibold">Error loading data:</p>
            </div>
            <p className="text-sm ml-8 whitespace-pre-wrap">{error}</p>
          </div>
        )}

        {!isLoading && !error && (
          <div>
            {/* Tabs for Runs, 4s, 6s */}
            <div className="mb-6 border-b border-border flex space-x-1">
              {['runs', 'fours', 'sixes'].map((tab) => (
                <button
                  key={tab}
                  onClick={() => setActiveTab(tab as 'runs' | 'fours' | 'sixes')}
                  className={`px-4 py-2 -mb-px border-b-2 font-medium text-sm transition-colors
                    ${activeTab === tab 
                      ? 'border-accent text-accent' 
                      : 'border-transparent text-textSecondary hover:border-gray-400 hover:text-textPrimary'}
                  `}
                >
                  {tab === 'runs' && <TrendingUp className="inline-block mr-2 h-4 w-4" />}
                  {tab === 'fours' && <Target className="inline-block mr-2 h-4 w-4" />}
                  {tab === 'sixes' && <TrendingDown className="inline-block mr-2 h-4 w-4 transform rotate-180 scale-x-[-1]" />} {/* Custom 6s icon idea */}
                  {tab.charAt(0).toUpperCase() + tab.slice(1)} Stats
                </button>
              ))}
            </div>

            {/* Content for active tab */} 
            {activeTab === 'runs' && (
              <>
                {runsStats && <PlayerPropStatsTable title={`Runs Scored Hit Rates (Default: ${defaultRunThresholds})`} data={runsStats.stats} />}
                {isLoadingCustomRuns && 
                  <div className="flex justify-center items-center min-h-[100px]">
                    <Loader2 className="h-8 w-8 animate-spin text-yellow-400" />
                    <p className="ml-3 text-md text-textSecondary">Loading custom run stats...</p>
                  </div>
                }
                {customRunsStats && customRunsStats.stats.length > 0 && (
                  <PlayerPropStatsTable 
                    title={`Custom Run Props (Thresholds: ${appliedCustomRunThresholds})`} 
                    data={customRunsStats.stats} 
                    isCustom={true} 
                  />
                )}
                 {customRunsStats && customRunsStats.stats.length === 0 && appliedCustomRunThresholds && (
                  <p className="text-center text-textSecondary py-4 bg-gray-800 rounded-md my-4">No data found for custom run thresholds: {appliedCustomRunThresholds} with current filters.</p>
                )}
                {includeGamelog && runsStats?.gamelog && runsStats.gamelog.length > 0 && (
                  <PlayerGamelogTable gamelog={runsStats.gamelog} title="Runs Gamelog (Default Thresholds)" />
                )}
                {includeGamelog && customRunsStats?.gamelog && customRunsStats.gamelog.length > 0 && (
                  <PlayerGamelogTable gamelog={customRunsStats.gamelog} title={`Runs Gamelog (Custom: ${appliedCustomRunThresholds})`} isCustomGamelog={true} />
                )}
              </>
            )}
            {activeTab === 'fours' && (
              <>
                {foursStats && <PlayerPropStatsTable title="4s Scored Hit Rates" data={foursStats.stats} boundaryType="4s" />}
                {includeGamelog && foursStats?.gamelog && foursStats.gamelog.length > 0 && (
                  <PlayerGamelogTable gamelog={foursStats.gamelog} title="4s Gamelog" />
                )}
              </>
            )}
            {activeTab === 'sixes' && (
              <>
                {sixesStats && <PlayerPropStatsTable title="6s Scored Hit Rates" data={sixesStats.stats} boundaryType="6s" />}
                {includeGamelog && sixesStats?.gamelog && sixesStats.gamelog.length > 0 && (
                  <PlayerGamelogTable gamelog={sixesStats.gamelog} title="6s Gamelog" />
                )}
              </>
            )}
            
            {/* Common message if gamelog is toggled but no data for any active stat type */}, 
            {includeGamelog && 
              activeTab === 'runs' && (!runsStats?.gamelog || runsStats.gamelog.length === 0) && (!customRunsStats?.gamelog || customRunsStats.gamelog.length === 0) && 
              <p className="text-center text-textSecondary py-4">No gamelog data available for Runs with current filters.</p>
            }
            {includeGamelog && 
              activeTab === 'fours' && (!foursStats?.gamelog || foursStats.gamelog.length === 0) && 
              <p className="text-center text-textSecondary py-4">No gamelog data available for 4s with current filters.</p>
            }
            {includeGamelog && 
              activeTab === 'sixes' && (!sixesStats?.gamelog || sixesStats.gamelog.length === 0) && 
              <p className="text-center text-textSecondary py-4">No gamelog data available for 6s with current filters.</p>
            }
          </div>
        )}
      </div>
    </div>
  );
}

// --- Sub-component for displaying stats table ---
interface PlayerPropStatsTableProps {
  title: string;
  data: ThresholdStat[];
  boundaryType?: '4s' | '6s'; // Optional: to customize header for boundaries
  isCustom?: boolean; // To style custom rows
}

function PlayerPropStatsTable({ title, data, boundaryType, isCustom = false }: PlayerPropStatsTableProps) {
  if (!data || data.length === 0) {
    return <p className="text-center text-textSecondary py-4">No {boundaryType || 'runs'} stats data available for the current filters {isCustom ? "(custom)" : ""}.</p>;
  }

  return (
    <div className="mb-8 bg-background p-3 sm:p-4 rounded-lg shadow">
      <h3 className="text-xl font-semibold text-accent mb-4 flex items-center">
        {boundaryType === '4s' && <Target className="mr-2 h-5 w-5"/>}
        {boundaryType === '6s' && <TrendingDown className="mr-2 h-5 w-5 transform rotate-180 scale-x-[-1]" />}
        {!boundaryType && <BarChart2 className="mr-2 h-5 w-5"/>}
        {title}
      </h3>
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-border">
          <thead className="bg-muted">
            <tr>
              <th scope="col" className="px-4 py-3 text-left text-xs font-medium text-textSecondary uppercase tracking-wider">Threshold ({boundaryType ? 'N+' : 'N+'})</th>
              <th scope="col" className="px-4 py-3 text-left text-xs font-medium text-textSecondary uppercase tracking-wider">Innings</th>
              <th scope="col" className="px-4 py-3 text-left text-xs font-medium text-textSecondary uppercase tracking-wider">Met Threshold</th>
              <th scope="col" className="px-4 py-3 text-left text-xs font-medium text-textSecondary uppercase tracking-wider">Hit Rate (%)</th>
            </tr>
          </thead>
          <tbody className="bg-card divide-y divide-border">
            {data.map((stat) => (
              <tr key={`${stat.threshold}-${isCustom ? 'custom' : 'default'}`} 
                  className={`transition-colors ${isCustom ? 'bg-yellow-900/30 hover:bg-yellow-800/40' : 'hover:bg-hover'}`}>
                <td className={`px-4 py-3 whitespace-nowrap text-sm font-medium ${isCustom ? 'text-yellow-300' : 'text-textPrimary'}`}>{stat.threshold}+</td>
                <td className={`px-4 py-3 whitespace-nowrap text-sm ${isCustom ? 'text-gray-300' : 'text-textSecondary'}`}>{stat.total_innings_for_criteria}</td>
                <td className={`px-4 py-3 whitespace-nowrap text-sm ${isCustom ? 'text-gray-300' : 'text-textSecondary'}`}>{stat.innings_met_or_exceeded_threshold}</td>
                <td className={`px-4 py-3 whitespace-nowrap text-sm font-semibold 
                  ${isCustom ? (stat.hit_rate_percentage >= 50 ? 'text-green-400' : stat.hit_rate_percentage >= 25 ? 'text-yellow-400' : 'text-red-400') 
                              : (stat.hit_rate_percentage >= 50 ? 'text-green-500' : stat.hit_rate_percentage >= 25 ? 'text-yellow-500' : 'text-red-500')}`}>
                  {stat.hit_rate_percentage.toFixed(2)}%
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

PlayerPropStatsTable.displayName = "PlayerPropStatsTable";

// --- Sub-component for displaying Gamelog table ---
interface PlayerGamelogTableProps {
  gamelog: PlayerGamelogEntry[];
  title: string;
  isCustomGamelog?: boolean;
}

function PlayerGamelogTable({ gamelog, title, isCustomGamelog = false }: PlayerGamelogTableProps) {
  if (!gamelog || gamelog.length === 0) {
    return <p className="text-center text-textSecondary py-4">No gamelog data available for {title}.</p>;
  }

  // Helper to format date string
  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleDateString('en-US', {
      year: 'numeric', month: 'short', day: 'numeric'
    });
  };

  const displayCellContent = (value: string | number | null | undefined, placeholder: string = '-') => {
    if (value === null || value === undefined || value === '') return placeholder;
    return value;
  };

  return (
    <div className={`mt-8 mb-8 ${isCustomGamelog ? 'bg-yellow-900/20' : 'bg-background'} p-3 sm:p-4 rounded-lg shadow`}>
      <h3 className={`text-xl font-semibold ${isCustomGamelog ? 'text-yellow-300' : 'text-accent'} mb-4 flex items-center`}>
        <ListChecks className="mr-2 h-5 w-5" /> {title}
      </h3>
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-border">
          <thead className={`${isCustomGamelog ? 'bg-yellow-800/30' : 'bg-muted'}`}>
            <tr>
              <th scope="col" className="px-3 py-3 text-left text-xs font-medium text-textSecondary uppercase tracking-wider">Date</th>
              <th scope="col" className="px-3 py-3 text-left text-xs font-medium text-textSecondary uppercase tracking-wider">Batting Team</th>
              <th scope="col" className="px-3 py-3 text-left text-xs font-medium text-textSecondary uppercase tracking-wider">Opposition</th>
              <th scope="col" className="px-3 py-3 text-left text-xs font-medium text-textSecondary uppercase tracking-wider">City</th>
              <th scope="col" className="px-3 py-3 text-left text-xs font-medium text-textSecondary uppercase tracking-wider">Inn</th>
              <th scope="col" className="px-3 py-3 text-left text-xs font-medium text-textSecondary uppercase tracking-wider">Runs</th>
              <th scope="col" className="px-3 py-3 text-left text-xs font-medium text-textSecondary uppercase tracking-wider">Balls</th>
              <th scope="col" className="px-3 py-3 text-left text-xs font-medium text-textSecondary uppercase tracking-wider">4s</th>
              <th scope="col" className="px-3 py-3 text-left text-xs font-medium text-textSecondary uppercase tracking-wider">6s</th>
              <th scope="col" className="px-3 py-3 text-left text-xs font-medium text-textSecondary uppercase tracking-wider">SR</th>
              <th scope="col" className="px-3 py-3 text-left text-xs font-medium text-textSecondary uppercase tracking-wider">Wicket Type</th>
              <th scope="col" className="px-3 py-3 text-left text-xs font-medium text-textSecondary uppercase tracking-wider">Dismissed By</th>
              <th scope="col" className="px-3 py-3 text-left text-xs font-medium text-textSecondary uppercase tracking-wider">Fielders</th>
            </tr>
          </thead>
          <tbody className={`${isCustomGamelog ? 'divide-yellow-700/30' : 'divide-border'} ${isCustomGamelog ? 'bg-yellow-950/30' : 'bg-card'}`}>
            {gamelog.map((entry) => (
              <tr key={entry.match_id + "-" + entry.innings_no} className={`transition-colors ${isCustomGamelog ? 'hover:bg-yellow-800/50' : 'hover:bg-hover'}`}>
                <td className={`px-3 py-2 whitespace-nowrap text-sm ${isCustomGamelog ? 'text-gray-300' : 'text-textSecondary'}`}>{formatDate(entry.match_date)}</td>
                <td className={`px-3 py-2 whitespace-nowrap text-sm ${isCustomGamelog ? 'text-gray-200' : 'text-textPrimary'}`}>{displayCellContent(entry.batting_team)}</td>
                <td className={`px-3 py-2 whitespace-nowrap text-sm ${isCustomGamelog ? 'text-gray-200' : 'text-textPrimary'}`}>{displayCellContent(entry.bowling_team)}</td>
                <td className={`px-3 py-2 whitespace-nowrap text-sm ${isCustomGamelog ? 'text-gray-300' : 'text-textSecondary'}`}>{displayCellContent(entry.city)}</td>
                <td className={`px-3 py-2 whitespace-nowrap text-sm ${isCustomGamelog ? 'text-gray-300' : 'text-textSecondary'}`}>{displayCellContent(entry.innings_no)}</td>
                <td className={`px-3 py-2 whitespace-nowrap text-sm font-semibold ${isCustomGamelog ? 'text-yellow-300' : 'text-textPrimary'}`}>{displayCellContent(entry.runs_scored)}</td>
                <td className={`px-3 py-2 whitespace-nowrap text-sm ${isCustomGamelog ? 'text-gray-300' : 'text-textSecondary'}`}>{displayCellContent(entry.balls_faced)}</td>
                <td className={`px-3 py-2 whitespace-nowrap text-sm ${isCustomGamelog ? 'text-gray-300' : 'text-textSecondary'}`}>{displayCellContent(entry.fours)}</td>
                <td className={`px-3 py-2 whitespace-nowrap text-sm ${isCustomGamelog ? 'text-gray-300' : 'text-textSecondary'}`}>{displayCellContent(entry.sixes)}</td>
                <td className={`px-3 py-2 whitespace-nowrap text-sm ${isCustomGamelog ? 'text-gray-300' : 'text-textSecondary'}`}>{entry.strike_rate !== null && entry.strike_rate !== undefined ? entry.strike_rate.toFixed(2) : '-'}</td>
                <td className={`px-3 py-2 whitespace-nowrap text-sm ${isCustomGamelog ? 'text-gray-400' : 'text-textSecondary'}`}>{displayCellContent(entry.wicket_type, 'Not Out')}</td>
                <td className={`px-3 py-2 whitespace-nowrap text-sm ${isCustomGamelog ? 'text-gray-400' : 'text-textSecondary'}`}>{displayCellContent(entry.wicket_bowler)}</td>
                <td className={`px-3 py-2 whitespace-nowrap text-sm ${isCustomGamelog ? 'text-gray-400' : 'text-textSecondary'}`}>{displayCellContent(entry.wicket_fielders)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
PlayerGamelogTable.displayName = "PlayerGamelogTable";

// Helper to get a unique key for map iterations
// Moved to main component scope to be accessible by all parts if needed
// const getSelectKey = (item: string | Tournament | Season | Venue | Team, index: number) => {
//   if (typeof item === 'string') return `${item}-${index}`;
//   if ('name' in item) return `${item.name}-${index}`;
//   if ('season' in item) return `${(item as Season).season}-${index}`;
//   return `key-${index}`;
// };

// TODO:
// - Implement Gamelog display -- DONE for player props page
// - Implement more sophisticated multi-select dropdowns for filters (e.g., using react-select or similar)
// - Debounce filter changes if auto-applying, or rely on "Apply Filters" button
// - Consider memoization for performance if component becomes complex
// - Styling refinement for tables and filters
// - Add clear/reset filters button 