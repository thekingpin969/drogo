import * as fs from "fs";
import axios from "axios";
import { Pool } from "pg";

const db = new Pool({
    connectionString: "postgresql://postgres:iUkFJNoHvhRqKIcRqvzPASdkwHLDIysg@shortline.proxy.rlwy.net:50535/railway",
    max: 20,
});

// ─── ScraperAPI Config ────────────────────────────────────────────────────────

const MAX_KEY_CREDITS = 5000;
const TRACKER_FILE = "scraper_keys_tracking.json";

function getTrackerState(): Record<string, number> {
    if (!fs.existsSync(TRACKER_FILE)) return {};
    try {
        return JSON.parse(fs.readFileSync(TRACKER_FILE, "utf-8"));
    } catch {
        return {};
    }
}

function recordCreditUsage(usedKey: string) {
    const state = getTrackerState();
    state[usedKey] = (state[usedKey] || 0) + 1;
    fs.writeFileSync(TRACKER_FILE, JSON.stringify(state, null, 2), "utf-8");
}

function getNextApiKey(): string {
    const rawKeys = process.env.SCRAPER_API_KEYS || "c2253d2579c7308168cabab6f011ee9f,646bb84f70e30a90928e1e9ba70cc907,31650147d1fa794b9e03b0809e98aefd,94b2ef48baf218ea358949384041aa8d";
    const keyArray = rawKeys.split(",").map(k => k.trim()).filter(k => k.length > 0);

    if (keyArray.length === 0) {
        throw new Error("Fatal: No Scraper API Keys bounded.");
    }

    const state = getTrackerState();
    for (const key of keyArray) {
        const usage = state[key] || 0;
        if (usage < MAX_KEY_CREDITS) {
            return key;
        }
    }
    throw new Error("💀 ALL ScraperAPI keys exhausted!");
}

function wrapScraperApi(targetUrl: string): { url: string; key: string } {
    const apiKey = getNextApiKey();
    const encoded = encodeURIComponent(targetUrl);
    return {
        url: `https://api.scraperapi.com/?api_key=${apiKey}&url=${encoded}&keep_headers=true&device_type=desktop`,
        key: apiKey,
    };
}

// ─── CSRF & Auth ─────────────────────────────────────────────────────────────

interface CsrfResult {
    csrf: string;
    sessionCookie: string;
}

function extractSetCookie(setCookieHeaders: string[], name: string): string | null {
    for (const header of setCookieHeaders) {
        const parts = header.split(";");
        const pair = (parts[0] ?? "").trim();
        if (pair.toLowerCase().startsWith(name.toLowerCase() + "=")) {
            return pair;
        }
    }
    return null;
}

function markKeyExhausted(key: string) {
    const state = getTrackerState();
    state[key] = MAX_KEY_CREDITS;
    fs.writeFileSync(TRACKER_FILE, JSON.stringify(state, null, 2), "utf-8");
    console.log(`[System] Key ${key} marked as exhausted in tracker.`);
}

async function fetchCsrf(): Promise<CsrfResult> {
    console.log("Fetching CSRF token...");
    try {
        const wrap = wrapScraperApi("https://www.zomato.com/webroutes/auth/csrf");
        const res = await axios.get(wrap.url, {
            headers: {
                "accept": "*/*",
                "accept-language": "en-US,en;q=0.9",
                "referer": "https://www.zomato.com/",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            },
        });

        recordCreditUsage(wrap.key);

        const setCookies = (res.headers["set-cookie"] as string[]) || [];
        const csrfPair = extractSetCookie(setCookies, "csrf");
        const csrf = (csrfPair ? csrfPair.split("=")[1] : "") ?? "";
        const sessionCookie = setCookies.join("; ");

        return { csrf, sessionCookie };
    } catch (err: any) {
        if (err.response?.status === 403 && (err.response?.data?.includes("exhausted") || (typeof err.response?.data === 'string' && err.response?.data?.includes("Credits")))) {
            const exhaustedKey = err.config.url.split('api_key=')[1].split('&')[0];
            markKeyExhausted(exhaustedKey);
            return fetchCsrf(); // Recursive retry with next key
        }
        throw err;
    }
}

// ─── Scraper Logic ───────────────────────────────────────────────────────────

async function fetchRestaurantDetails(pageUrl: string, csrfResult: CsrfResult): Promise<any> {
    const targetUrl = `https://www.zomato.com/webroutes/getPage?page_url=${encodeURIComponent(pageUrl)}&location=&isMobile=0`;
    const wrap = wrapScraperApi(targetUrl);
    
    const headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/json",
        "x-zomato-csrft": csrfResult.csrf,
        "cookie": csrfResult.sessionCookie,
        "referer": "https://www.zomato.com/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    };

    try {
        const res = await axios.get(wrap.url, { headers });
        recordCreditUsage(wrap.key);
        return res.data;
    } catch (err: any) {
        if (err.response?.status === 403 && (err.response?.data?.includes("exhausted") || (typeof err.response?.data === 'string' && err.response?.data?.includes("Credits")))) {
            const exhaustedKey = err.config.url.split('api_key=')[1].split('&')[0];
            markKeyExhausted(exhaustedKey);
            return fetchRestaurantDetails(pageUrl, csrfResult); // Retry with next key
        }
        throw err;
    }
}

function parseDetails(data: any) {
    if (!data || !data.page_data) return null;
    
    const sections = data.page_data.sections;
    const basicInfo = sections.SECTION_BASIC_INFO || {};
    const contact = sections.SECTION_RES_CONTACT || {};
    const menuData = data.page_data.order?.menuList?.menus || [];

    return {
        resId: basicInfo.res_id,
        name: basicInfo.name,
        location: {
            address: contact.address,
            latitude: contact.latitude,
            longitude: contact.longitude,
            locality: contact.locality_verbose,
            phone: contact.phoneDetails?.phoneStr
        },
        timing: basicInfo.timing?.timing_desc || basicInfo.timing?.customised_timings?.opening_hours?.[0]?.timing,
        menu: menuData.map((m: any) => ({
            category: m.menu.name,
            items: (m.menu.categories || []).flatMap((cat: any) => 
                (cat.category.items || []).map((i: any) => ({
                    name: i.item.name,
                    desc: i.item.desc,
                    price: i.item.price?.display_price || i.item.price?.default_price || "N/A"
                }))
            )
        }))
    };
}

// ─── Main Execution ──────────────────────────────────────────────────────────

const INPUT_FILE = "zomato_kozhikode_restaurants.json";
const OUTPUT_FILE = "zomato_restaurants_details.json";
const BATCH_SIZE = 5;

async function main() {
    console.log(`Connecting to database...`);
    
    // Ensure column exists for state tracking
    try {
        await db.query("ALTER TABLE zomato_restaurants ADD COLUMN IF NOT EXISTS details JSONB");
    } catch (e) {
        console.log("Note: Could not run ALTER TABLE (might lack permissions), continuing assuming column exists.");
    }

    let csrfResult = await fetchCsrf();
    let totalProcessed = 0;

    while (true) {
        // Fetch a batch of 5 unprocessed restaurants
        const { rows: batch } = await db.query(
            "SELECT res_id, name, data FROM zomato_restaurants WHERE details IS NULL LIMIT $1",
            [BATCH_SIZE]
        );

        if (batch.length === 0) {
            console.log("No more restaurants to process. Task complete!");
            break;
        }

        console.log(`\n--- Processing Batch of ${batch.length} (Total Processed: ${totalProcessed}) ---`);

        const promises = batch.map(async (row: any) => {
            const resId = row.res_id;
            const resName = row.name;
            const data = row.data || {};
            const pageUrl = data.order?.actionInfo?.clickUrl || data.cardAction?.clickUrl;

            if (!pageUrl) {
                console.log(`[Skip] No URL for ${resName} (${resId})`);
                // Mark as empty in DB to avoid repeated skips
                await db.query("UPDATE zomato_restaurants SET details = '{}'::jsonb WHERE res_id = $1", [resId]);
                return null;
            }

            try {
                console.log(`[Fetching] ${resName} (${resId})...`);
                const rawDetails = await fetchRestaurantDetails(pageUrl, csrfResult);
                const parsed = parseDetails(rawDetails);
                
                if (parsed) {
                    // 1. Update Database for state tracking
                    await db.query("UPDATE zomato_restaurants SET details = $1 WHERE res_id = $2", [JSON.stringify(parsed), resId]);
                    
                    // 2. Append to local JSON (JSON Lines format for safety)
                    fs.appendFileSync(OUTPUT_FILE, JSON.stringify(parsed) + "\n");
                    
                    console.log(`[Success] ${resName}`);
                    return parsed;
                }
            } catch (err: any) {
                console.error(`[Error] ${resName}: ${err.message}`);
            }
            return null;
        });

        await Promise.all(promises);
        totalProcessed += batch.length;

        // Small delay to be polite
        await new Promise(r => setTimeout(r, 1000));
        
        // Refresh CSRF occasionally for stability
        if (totalProcessed % 25 === 0) {
            csrfResult = await fetchCsrf();
        }
    }

    console.log(`Finished processing. Total attempted in this session: ${totalProcessed}`);
    process.exit(0);
}

main().catch(console.error);
