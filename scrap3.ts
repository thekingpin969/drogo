/**
 * Zomato Restaurant Scraper
 *
 * Endpoint: POST https://www.zomato.com/webroutes/search/home
 * Auth:     GET  https://www.zomato.com/webroutes/auth/csrf  → x-zomato-csrft header
 * Pagination: driven by `searchMetaData.postbackParams` returned in each response
 *
 * Usage:
 *   npx ts-node zomato-scraper.ts
 *   npx ts-node zomato-scraper.ts --city kozhikode --entityId 11296 --lat 11.258792 --lon 75.780387
 */

import * as fs from "fs";
import { Pool } from "pg";

const db = new Pool({
    connectionString: "postgresql://postgres:iUkFJNoHvhRqKIcRqvzPASdkwHLDIysg@shortline.proxy.rlwy.net:50535/railway",
    max: 20, // default max capacity pool securely covering parallel concurrency locks
});

// ─── ScraperAPI Settings ────────────────────────────────────────────────────────

// const SCRAPER_API_KEY = "cc71b1ba9f3ac895e55c266b8963fd2d";
const SCRAPER_API_KEY_ARRAY = [
    "c2253d2579c7308168cabab6f011ee9f",
    "646bb84f70e30a90928e1e9ba70cc907",
    "31650147d1fa794b9e03b0809e98aefd",
    "94b2ef48baf218ea358949384041aa8d"
]

let currentApiKeyIndex = 0;

function getNextApiKey(): any {
    const key = SCRAPER_API_KEY_ARRAY[currentApiKeyIndex];
    currentApiKeyIndex = (currentApiKeyIndex + 1) % SCRAPER_API_KEY_ARRAY.length;
    return key;
}

function wrapScraperApi(targetUrl: string): string {
    const apiKey = getNextApiKey();
    const encoded = encodeURIComponent(targetUrl);
    return `https://api.scraperapi.com/?api_key=${apiKey}&url=${encoded}&keep_headers=true&device_type=desktop`;
}

// ─── Config ──────────────────────────────────────────────────────────────────

interface CityConfig {
    cityName: string;
    entityId: number;
    cityId: number;
    latitude: string;
    longitude: string;
    placeId: string;
    cellId: string;
    deliverySubzoneId: number;
    countryId: number;
    countryName: string;
}

// Default: Kozhikode (extracted from HAR)
const DEFAULT_CITY: CityConfig = {
    cityName: "Kozhikode",
    entityId: 11296,
    cityId: 11296,
    latitude: "11.2587920000000000",
    longitude: "75.7803870000000000",
    placeId: "29601",
    cellId: "4298220988203532288",
    deliverySubzoneId: 29601,
    countryId: 1,
    countryName: "India",
};

const BASE_URL = "https://www.zomato.com";
const DELAY_MS = 800; // polite delay between pages

// ─── Types ───────────────────────────────────────────────────────────────────

interface SearchMetaData {
    previousSearchParams: string;
    postbackParams: string;
    totalResults: number;
    hasMore: boolean;
    getInactive: boolean;
}

interface PostbackParams {
    search_id?: string;
    processed_chain_ids: number[];
    shown_res_count: number;
}

// Raw restaurant record — every field from the API is preserved
type Restaurant = Record<string, unknown>;

interface SearchResponse {
    sections: {
        SECTION_SEARCH_RESULT: RestaurantCard[];
        SECTION_SEARCH_META_INFO: {
            searchMetaData: SearchMetaData;
        };
    };
}

// Keep the card type loose so nothing is accidentally dropped
interface RestaurantCard {
    type: string;
    info: Record<string, unknown> & { resId: number };
    isPromoted?: boolean;
    [key: string]: unknown;
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function buildFilters(
    meta: SearchMetaData,
    appliedFilter = true
): string {
    const filters = {
        searchMetadata: {
            previousSearchParams: meta.previousSearchParams,
            postbackParams: meta.postbackParams,
            totalResults: meta.totalResults,
            hasMore: meta.hasMore,
            getInactive: false,
        },
        dineoutAdsMetaData: {},
        appliedFilter: appliedFilter
            ? [
                // {
                //     filterType: "category_sheet",
                //     filterValue: "delivery_home",
                //     isHidden: true,
                //     isApplied: true,
                //     postKey: JSON.stringify({ category_context: "delivery_home" }),
                // },
                {
                    filterType: "sort",
                    filterValue: "popularity_desc",
                    isDefault: true,
                    postKey: JSON.stringify({ sort: "popularity_desc" }),
                    isApplied: true,
                },
            ]
            : [],
        urlParamsForAds: {},
    };
    return JSON.stringify(filters);
}

function buildInitialMeta(): SearchMetaData {
    const previousSearchParams = JSON.stringify({
        PreviousSearchFilter: [
            JSON.stringify({ category_context: "delivery_home" }),
            "",
            JSON.stringify({ sort: "popularity_desc" }),
        ],
    });
    const postbackParams = JSON.stringify({
        processed_chain_ids: [] as number[],
        shown_res_count: 0,
    });
    return {
        previousSearchParams,
        postbackParams,
        totalResults: 0,
        hasMore: true,
        getInactive: false,
    };
}

function buildRequestBody(city: CityConfig, meta: SearchMetaData) {
    return {
        context: "delivery",
        filters: buildFilters(meta),
        addressId: 0,
        entityId: city.entityId,
        entityType: "city",
        locationType: "",
        isOrderLocation: 1,
        cityId: city.cityId,
        latitude: city.latitude,
        longitude: city.longitude,
        userDefinedLatitude: parseFloat(city.latitude),
        userDefinedLongitude: parseFloat(city.longitude),
        entityName: city.cityName,
        orderLocationName: city.cityName,
        cityName: city.cityName,
        countryId: city.countryId,
        countryName: city.countryName,
        displayTitle: city.cityName,
        o2Serviceable: true,
        placeId: city.placeId,
        cellId: city.cellId,
        deliverySubzoneId: city.deliverySubzoneId,
        placeType: "DSZ",
        placeName: city.cityName,
        isO2City: true,
        fetchFromGoogle: false,
        fetchedFromCookie: false,
        isO2OnlyCity: true,
        address_template: [],
        otherRestaurantsUrl: "",
    };
}

/**
 * Returns the full raw card data — no fields are omitted.
 * The top-level `isPromoted` flag is merged into the object alongside `info`.
 */
function parseRestaurant(card: RestaurantCard): Restaurant | null {
    if (card.type !== "restaurant") return null;
    // Spread the entire card so every field (including future ones) is kept
    const { info, isPromoted, ...rest } = card;
    return card;
}

// ─── Core Scraper ─────────────────────────────────────────────────────────────

interface CsrfResult {
    csrf: string;
    sessionCookie: string; // extracted from Set-Cookie response headers
}

/**
 * Extract a named cookie value from an array of Set-Cookie header strings.
 * Returns the raw "Name=Value" pair (no attributes).
 */
function extractSetCookie(setCookieHeaders: string[], name: string): string | null {
    for (const header of setCookieHeaders) {
        // Each header looks like: "NAME=VALUE; Path=/; ..."
        const parts = header.split(";");
        const pair = (parts[0] ?? "").trim(); // "NAME=VALUE"
        if (pair.toLowerCase().startsWith(name.toLowerCase() + "=")) {
            return pair;
        }
    }
    return null;
}

async function fetchCsrf(city: CityConfig): Promise<CsrfResult> {
    const logPrefix = `[${city.cityName}]`;
    console.log(`${logPrefix} [1] Fetching CSRF token...`);
    const res = await fetch(wrapScraperApi(`${BASE_URL}/webroutes/auth/csrf`), {
        headers: {
            accept: "*/*",
            "accept-language": "en-US,en;q=0.9",
            referer: `${BASE_URL}/restaurants`,
            "user-agent":
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        },
    });

    if (!res.ok) {
        throw new Error(`CSRF fetch failed: ${res.status} ${res.statusText}`);
    }

    // Collect all Set-Cookie headers from the CSRF response
    const setCookieHeaders: string[] = [];
    res.headers.forEach((value, key) => {
        if (key.toLowerCase() === "set-cookie") {
            setCookieHeaders.push(value);
        }
    });

    // In Bun/Node, getSetCookie() is the reliable API when available
    const allSetCookies: string[] =
        typeof (res.headers as any).getSetCookie === "function"
            ? (res.headers as any).getSetCookie()
            : setCookieHeaders;

    // Extract important session cookies to forward
    const phpSessId = extractSetCookie(allSetCookies, "PHPSESSID");
    const csrfCookie = extractSetCookie(allSetCookies, "csrf");

    const cookieParts: string[] = [];
    if (phpSessId) cookieParts.push(phpSessId);
    if (csrfCookie) cookieParts.push(csrfCookie);
    const sessionCookie = cookieParts.join("; ");

    console.log(`${logPrefix}     ✓ Session cookies from CSRF response: ${sessionCookie || "(none)"}`);

    const data = (await res.json()) as { csrf: string };
    console.log(`${logPrefix}     ✓ CSRF token: ${data.csrf}`);
    return { csrf: data.csrf, sessionCookie };
}

/** Shared headers used for every Zomato request */
function buildZomatoHeaders(
    csrf: string,
    sessionCookie: string,
    city: CityConfig
): Record<string, string> {
    return {
        accept: "*/*",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/json",
        origin: BASE_URL,
        referer: `${BASE_URL}/${city.cityName.toLowerCase()}/restaurants`,
        "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        "x-zomato-csrft": csrf,
        ...(sessionCookie ? { cookie: sessionCookie } : {}),
    };
}

/**
 * Single attempt: fetch one page, wrapped securely through ScraperAPI.
 */
async function fetchPage(
    csrf: string,
    sessionCookie: string,
    city: CityConfig,
    meta: SearchMetaData,
    page: number
): Promise<SearchResponse> {
    const bodyStr = JSON.stringify(buildRequestBody(city, meta));
    const headers = buildZomatoHeaders(csrf, sessionCookie, city);
    const url = `${BASE_URL}/webroutes/search/home`;

    // Direct request wrapped with ScraperAPI — use Bun's native fetch
    const res = await fetch(wrapScraperApi(url), {
        method: "POST",
        headers,
        body: bodyStr,
    });

    if (!res.ok) {
        const text = await res.text();
        throw new Error(
            `Page ${page} failed: ${res.status} ${res.statusText}\n${text.slice(0, 300)}`
        );
    }

    return res.json() as Promise<SearchResponse>;
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function scrapeLocation(
    city: CityConfig,
    allRestaurants: Restaurant[]
) {
    const logPrefix = `[${city.cityName}]`;
    console.log(`\n${logPrefix} 🍽  Zomato Scraper — Location Initialization (Lat: ${city.latitude}, Lng: ${city.longitude})`);

    // Fetch unique auth cookies per location
    await sleep(DELAY_MS * 2); // Initial reliable delay avoiding rate limits
    let { csrf, sessionCookie } = await fetchCsrf(city);

    let meta = buildInitialMeta();
    let page = 1;

    while (true) {
        const postback: PostbackParams = JSON.parse(meta.postbackParams);
        console.log(`${logPrefix} [page ${page}] Fetching... (shown so far: ${postback.shown_res_count}, total: ${meta.totalResults})`);

        let response: SearchResponse | undefined;
        let attempt = 0;
        const MAX_RETRIES = 5;
        let success = false;

        while (attempt < MAX_RETRIES) {
            try {
                await sleep(DELAY_MS); // Reliable delay with each page request
                response = await fetchPage(csrf, sessionCookie, city, meta, page);
                success = true;
                break; // Break strictly out of retry loop
            } catch (err: any) {
                attempt++;
                let isAuthError = false;
                const errStr = String(err);
                if (err instanceof AggregateError) {
                    isAuthError = err.errors.some((e: any) => String(e).includes("401") || String(e).includes("Unauthorized"));
                } else {
                    isAuthError = errStr.includes("401") || errStr.includes("Unauthorized");
                }

                if (isAuthError) {
                    console.warn(`${logPrefix} ⚠️ Unauthorized response! Refreshing CSRF... (${attempt}/${MAX_RETRIES})`);
                    try {
                        await sleep(DELAY_MS * 3);
                        const refreshed = await fetchCsrf(city);
                        csrf = refreshed.csrf;
                        sessionCookie = refreshed.sessionCookie;
                    } catch (e) {
                        console.error(`${logPrefix} ❌ Failed to refresh CSRF:`, e);
                    }
                } else {
                    console.error(`${logPrefix} ✗ Page request failed (${attempt}/${MAX_RETRIES}):`, errStr.slice(0, 100));
                    await sleep(DELAY_MS * 2);
                }
            }
        }

        if (!success || !response) {
            console.error(`${logPrefix} ❌ Reached max ${MAX_RETRIES} failures on page ${page}. Bailing completely from this location.`);
            break; // Break completely from pagination while-block
        }

        const cards = response.sections?.SECTION_SEARCH_RESULT ?? [];
        const newMeta = response.sections?.SECTION_SEARCH_META_INFO?.searchMetaData;

        // Parse restaurants from this page and map concurrently against PG for dynamic UPSERTING.
        let newCount = 0;
        let insertedToDb = 0;

        await Promise.all(cards.map(async (card: any) => {
            const r = parseRestaurant(card);
            if (!r) return;

            // Inject the source location matching user's request for future reference
            r._sourceLocation = {
                placeId: city.placeId,
                latitude: city.latitude,
                longitude: city.longitude,
                deliverySubzoneId: city.deliverySubzoneId,
                entityName: city.cityName // Fallback descriptor 
            };

            const rInfo = r.info as any;
            const resId = String(rInfo?.resId || "");
            const resName = String(rInfo?.name || "");

            if (resId && resName) {
                try {
                    const result = await db.query(
                        `INSERT INTO zomato_restaurants (res_id, name, data) 
                         VALUES ($1, $2, $3) 
                         ON CONFLICT (res_id, name) DO NOTHING`,
                        [resId, resName, JSON.stringify(r)]
                    );
                    if (result.rowCount && result.rowCount > 0) {
                        insertedToDb++;
                    }
                } catch (dbErr) {
                    console.error(`${logPrefix} ❌ Database write failure for ${resName}:`, dbErr);
                }
            }

            allRestaurants.push(r);
            newCount++;
        }));

        console.log(`${logPrefix}     ✓ Got ${newCount} on page (Upserted ${insertedToDb} entirely new to DB, instance memory array: ${allRestaurants.length})`);

        // Check pagination
        if (!newMeta || !newMeta.hasMore) {
            console.log(`${logPrefix}     ✓ No more pages. Fully exhausted Location.`);
            break;
        }

        // Verify we actually advanced (guard against infinite loops)
        const newPostback: PostbackParams = JSON.parse(newMeta.postbackParams);
        if (newPostback.shown_res_count <= postback.shown_res_count) {
            console.log(`${logPrefix}     ✓ shown_res_count did not advance — stopping.`);
            break;
        }

        meta = newMeta;
        page++;
        await sleep(DELAY_MS);
    }
}

// ─── Main ─────────────────────────────────────────────────────────────────────

(async () => {
    try {
        const locFilePath = "kozhikode_locations_output.json";
        if (!fs.existsSync(locFilePath)) {
            console.error(`Missing locations file: ${locFilePath}. Please run batchLocations first.`);
            process.exit(1);
        }

        let locData = JSON.parse(fs.readFileSync(locFilePath, "utf-8"));

        // Find max split id to iterate over
        const hasSplits = locData.some((l: any) => typeof l.splitid === "number");
        if (!hasSplits) {
            console.error(`❌ No "splitid" field found natively inside ${locFilePath}. Run your JS chunking script first!`);
            process.exit(1);
        }

        const maxSplitId = Math.max(...locData.map((l: any) => l.splitid || 1));
        console.log(`Loaded all ${locData.length} locations. Discovered ${maxSplitId} Splits. Starting concurrent architecture job...`);

        // Initialize PG Table securely for strictly deduplicated raw payload insertions
        await db.query(`
            CREATE TABLE IF NOT EXISTS zomato_restaurants (
                id SERIAL PRIMARY KEY,
                res_id VARCHAR(50) NOT NULL,
                name VARCHAR(255) NOT NULL,
                data JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(res_id, name)
            );
        `);
        console.log("🐘 Initialized PostgreSQL Database connection & ensured 'zomato_restaurants' table exists.");

        const allRestaurants: Restaurant[] = [];
        const masterFile = "zomato_kozhikode_master.json";
        const processedFile = "kozhikode_processed_locations.json";

        // Iterating cleanly through splits sequentially.
        for (let splitId = 1; splitId <= maxSplitId; splitId++) {
            const chunkTargetLocs = locData.filter((l: any) => l.splitid === splitId);

            if (chunkTargetLocs.length === 0) continue;

            console.log(`\n================================================================`);
            console.log(`🚀 STARTING BATCH: SPLIT #${splitId}/${maxSplitId} (Fanning out ${chunkTargetLocs.length} parallel instances)`);
            console.log(`================================================================`);

            // Concurrently process every location matching this split ID
            await Promise.all(chunkTargetLocs.map(async (item: any) => {
                const loc = item.response?.locationDetails;
                if (!loc) {
                    console.log(`[Split ${splitId}] Skipping malformed/empty location...`);
                    return;
                }

                const city: CityConfig = {
                    ...DEFAULT_CITY,
                    latitude: String(loc.latitude),
                    longitude: String(loc.longitude),
                };
                city.cityName = loc.entityName || loc.placeId || city.cityName;

                try {
                    // Fire autonomous concurrent execution block
                    await scrapeLocation(city, allRestaurants);

                    // Successfully scraped! Save the location strictly for future references.
                    // Because Node.js handles readFileSync and writeFileSync entirely synchronously in the primary event loop thread, 
                    // this completely protects against concurrent data race corruptions when processing in parallel. 
                    let processedData: any[] = [];
                    if (fs.existsSync(processedFile)) {
                        processedData = JSON.parse(fs.readFileSync(processedFile, "utf-8"));
                    }
                    processedData.push(loc);
                    fs.writeFileSync(processedFile, JSON.stringify(processedData, null, 2), "utf-8");

                } catch (failErr: any) {
                    console.error(`[${city.cityName}] ❌ Catastrophic instance escape exception:`, failErr);
                }
            }));

            // Force dump the centralized master checkpoint immediately after ALL parallel promises map for a single Split resolves.
            fs.writeFileSync(masterFile, JSON.stringify(allRestaurants, null, 2), "utf-8");
            console.log(`💾 Split #${splitId} globally resolved. Synced master checkpoint successfully with array length: ${allRestaurants.length}\n`);
        }

        console.log(`\n✅ Batch processing completely finished! Total dumped into file strictly via ScraperAPI concurrently: ${allRestaurants.length}`);
        
        await db.end();
        console.log(`🐘 PostgreSQL connection pool flushed securely.`);
    } catch (err) {
        console.error("Fatal exception boundary reached layout main loop:", err);
        process.exit(1);
    }
})();