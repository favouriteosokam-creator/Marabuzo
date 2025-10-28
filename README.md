//@version=6
indicator("Marubozu Quantum Coherence Detector — Production (v6) — Hardened", overlay=true, max_labels_count=500, max_lines_count=800)

// ==============================
// CONFIG / INPUTS
// ==============================
JR                       = input.float(0.142, "Judgment Ratio (1/7)", step=0.001)
lookback_mean            = input.int(6, "Past bars for 1/7 sequence", minval=2)
rsi_len                  = input.int(14, "RSI Length")
bb_len                   = input.int(13, "Bull/Bear Smooth")
atr_len                  = input.int(14, "ATR Length")
body_ratio_th            = input.float(0.85, "Marubozu Body Ratio Threshold", step=0.01)
body_ratio_th_v5         = input.float(0.90, "Legacy V5 Body Ratio", step=0.01)
wick_pct_th_v5           = input.float(0.05, "Legacy V5 Wick % of Body", step=0.01)
weights_rsi              = input.float(0.30, "W: RSI")
weights_vol              = input.float(0.25, "W: Volume")
weights_bullbear         = input.float(0.25, "W: Bull/Bear")
weights_range            = input.float(0.20, "W: Range")
min_vol_mult             = input.float(0.5, "Min volume multiplier", step=0.1)
min_range_mult           = input.float(0.3, "Min range multiplier (ATR)", step=0.05)
bb_method_options        = input.string("VolumeWeighted", "Bull/Bear Method", options=["VolumeWeighted", "BullsBearsPower"])
enable_alerts            = input.bool(true, "Enable Alerts (use alertcondition)")
agree_threshold          = input.int(2, "Multi-TF Agreement Threshold", minval=1)
export_rows              = input.int(40, "Export table rows", minval=4, maxval=200)
phase_coh_thresh         = input.float(0.75, "Phase coherence threshold", step=0.01)
plot_coherence           = input.bool(true, "Plot Quantum Coherence")
shade_bg                 = input.bool(true, "Shade background by coherence")

// Order block & slice options (conservative defaults)
enable_orderblocks_cfg   = input.bool(true, "Enable Order Block Detection (config)")
ob_lookback              = input.int(6, "Order Block Lookback Bars", minval=1)
ob_min_cluster           = input.int(2, "Min Marubozu cluster size for OB", minval=1)
ob_zone_padding          = input.float(0.001, "Order Block padding (price fraction)")
ob_max_zones             = input.int(6, "Max Order Blocks to keep", minval=1)
ob_decay_bars            = input.int(240, "OB decay bars (lifetime)", minval=10)
ob_merge_fuzz            = input.float(0.001, "OB merge fuzz (fraction)", step=0.0001)
enable_slices            = input.bool(true, "Enable Slice Segmentation")
slice_enter_threshold    = input.float(0.6, "Slice enter threshold", step=0.01)
slice_exit_threshold     = input.float(0.35, "Slice exit threshold", step=0.01)
slice_sustain_bars       = input.int(3, "Slice sustain bars", minval=1)
enable_4h_weight         = input.bool(true, "Enable 4H Decisiveness Weighting")
h4_weight_mult           = input.float(1.5, "4H weight multiplier", step=0.1)
label_rate_limit_bars    = input.int(5, "Min bars between major labels", minval=1)

// Performance / safety
max_request_calls        = input.int(120, "Max allowed estimated security calls", minval=20)
opt_run_optimizer        = input.bool(false, "Run tiny optimizer (last-bar, grid search)")
opt_grid_steps           = input.int(7, "Optimizer grid steps (hidden_factor)", minval=3, maxval=15)
opt_eval_bars            = input.int(240, "Optimizer eval bars", minval=60)
hidden_factor_default    = input.float(0.5, "Hidden Residual Factor (optimizer base)", step=0.01, minval=0.1, maxval=1.0)

// ==============================
// TIMEFRAMES & CONSTANTS
// ==============================
tfs      = array.from("1", "15", "60", "240", "D")
tf_names = array.from("1m", "15m", "1h", "4h", "Daily")
tf_count = array.size(tfs)
eps      = 1e-9

// ==============================
// CORE HELPERS
// ==============================
f_bullbear(o,h,l,c,v,method,len) =>
    pos_move = c - o
    bull_raw = pos_move > 0 ? pos_move * v : 0.0
    bear_raw = pos_move < 0 ? -pos_move * v : 0.0
    if method == "VolumeWeighted"
        bull = ta.sma(bull_raw, len)
        bear = ta.sma(bear_raw, len)
    else
        fast_ema = ta.ema(c, len)
        bull_power = math.max(h - fast_ema, 0.0)
        bear_power = math.max(fast_ema - l, 0.0)
        bull = ta.sma(bull_power, len)
        bear = ta.sma(bear_power, len)
    [bull, bear]

f_energy_for_bar(o,h,l,c,v) =>
    rsi_val = ta.rsi(c, rsi_len)
    rsi_term = math.abs((rsi_val - 50) / 50)
    vol_avg = ta.sma(v, lookback_mean)
    vol_term = vol_avg == 0 ? 0.0 : math.sqrt(math.min(v / vol_avg, 10.0))
    [bull, bear] = f_bullbear(o,h,l,c,v, bb_method_options, bb_len)
    bullbear_term = (bull + bear == 0) ? 0.0 : math.abs((bull - bear) / (bull + bear))
    range_tf = h - l
    atr_val = ta.atr(atr_len)
    range_term = atr_val == 0 ? 0.0 : math.tanh(math.min(range_tf / atr_val, 5.0) / 2.0)
    weights_rsi * rsi_term + weights_vol * vol_term + weights_bullbear * bullbear_term + weights_range * range_term

f_lower_tf_of(tf) =>
    tf == "D" ? "240" : tf == "240" ? "60" : tf == "60" ? "15" : tf == "15" ? "1" : ""

// robust marubozu geometry
f_is_marubozu_geom_o(o,h,l,c,body_ratio_th_local) =>
    body = math.abs(c - o)
    total_range = h - l
    if total_range == 0
        false
    else
        upper_wick = c > o ? h - c : h - o
        lower_wick = c > o ? o - l : c - l
        body_ratio = body / total_range
        body_ratio >= body_ratio_th_local and (upper_wick / (total_range + eps)) < 0.1 and (lower_wick / (total_range + eps)) < 0.1

f_is_marubozu_v5_o(o,h,l,c) =>
    body = c - o
    if body > 0
        wick_up = h - math.max(o, c)
        wick_down = math.min(o, c) - l
        body_thr = body_ratio_th_v5 * body
        wick_thr = wick_pct_th_v5 * body
        (body >= body_thr) and (wick_up <= wick_thr) and (wick_down <= wick_thr)
    else
        body2 = o - c
        if body2 > 0
            wick_up = h - math.max(o, c)
            wick_down = math.min(o, c) - l
            body_thr = body_ratio_th_v5 * body2
            wick_thr = wick_pct_th_v5 * body2
            (body2 >= body_thr) and (wick_up <= wick_thr) and (wick_down <= wick_thr)
        else
            false

// ==============================
// CANONICAL PER-TF STATE STRUCTURE (arrays)
// ==============================
var float[] state_Et         = array.new_float(tf_count, na)
var float[] state_Et_sma     = array.new_float(tf_count, na)
var float[] state_o          = array.new_float(tf_count, na)
var float[] state_h          = array.new_float(tf_count, na)
var float[] state_l          = array.new_float(tf_count, na)
var float[] state_c          = array.new_float(tf_count, na)
var float[] state_v          = array.new_float(tf_count, na)
var float[] state_vol_sma    = array.new_float(tf_count, na)
var float[] state_atr        = array.new_float(tf_count, na)
var float[] state_prop59     = array.new_float(tf_count, na)
var bool[]  state_maru_geom  = array.new_bool(tf_count, false)
var bool[]  state_maru_v5    = array.new_bool(tf_count, false)
// computed canonical fields
var float[] state_E_total    = array.new_float(tf_count, na)
var float[] state_mean_past  = array.new_float(tf_count, na)
var int[]   state_conf       = array.new_int(tf_count, 0)
var int[]   state_dir        = array.new_int(tf_count, 0)
var bool[]  state_final      = array.new_bool(tf_count, false)

// estimated calls tracker
var int estimated_calls = 0

// ==============================
// SINGLE-POINT CACHING: fetch all required series once per TF per bar
// returns estimated number of request.security calls used
// ==============================
f_fetch_and_cache_all() =>
    calls = 0
    for i = 0 to tf_count - 1
        tf = array.get(tfs, i)
        // compound ET and ET_SMA calls (2) + OHLCV+vols+atr (1) => 3 per TF
        Et_val     = request.security(syminfo.tickerid, tf, f_energy_for_bar(open, high, low, close, volume), lookahead=barmerge.lookahead_off, gaps=barmerge.gaps_off)
        Et_sma_val = request.security(syminfo.tickerid, tf, ta.sma(f_energy_for_bar(open, high, low, close, volume), lookback_mean), lookahead=barmerge.lookahead_off, gaps=barmerge.gaps_off)
        arr = request.security(syminfo.tickerid, tf, [open, high, low, close, volume, ta.sma(volume, lookback_mean), ta.atr(atr_len)], lookahead=barmerge.lookahead_off, gaps=barmerge.gaps_off)
        o_tf = arr[0]; h_tf = arr[1]; l_tf = arr[2]; c_tf = arr[3]; v_tf = arr[4]; vol_sma = arr[5]; atr_tf = arr[6]
        array.set(state_Et, i, Et_val)
        array.set(state_Et_sma, i, Et_sma_val)
        array.set(state_o, i, o_tf)
        array.set(state_h, i, h_tf)
        array.set(state_l, i, l_tf)
        array.set(state_c, i, c_tf)
        array.set(state_v, i, v_tf)
        array.set(state_vol_sma, i, vol_sma)
        array.set(state_atr, i, atr_tf)
        array.set(state_maru_geom, i, f_is_marubozu_geom_o(o_tf, h_tf, l_tf, c_tf, body_ratio_th))
        array.set(state_maru_v5, i, f_is_marubozu_v5_o(o_tf, h_tf, l_tf, c_tf))
        calls += 3
    // prop59 lower-TF calls: up to 2 per TF if lower TF exists
    for i = 0 to tf_count - 1
        tf = array.get(tfs, i)
        lower_tf = f_lower_tf_of(tf)
        if lower_tf == ""
            array.set(state_prop59, i, na)
        else
            p_now = request.security(syminfo.tickerid, lower_tf, f_energy_for_bar(open, high, low, close, volume), lookahead=barmerge.lookahead_off, gaps=barmerge.gaps_off)
            p_mean = request.security(syminfo.tickerid, lower_tf, ta.sma(f_energy_for_bar(open, high, low, close, volume), 59), lookahead=barmerge.lookahead_off, gaps=barmerge.gaps_off)
            prop = na(p_now) or na(p_mean) or p_mean == 0 ? na : p_now / p_mean
            array.set(state_prop59, i, prop)
            calls += 2
    estimated_calls := calls
    calls

// ==============================
// ORDER BLOCK POOL (bounded) — use box ids and update only when zones change
// ==============================
type OBZone
    float top
    float bot
    int   origin_bar
    int   tf_index
    int   created_bar
    int   last_seen_bar

var OBZone[] ob_zones = array.new<OBZone>()
var int[] ob_box_ids = array.new_int() // store box ids (0 means none)

f_ob_decay_and_clean() =>
    i = 0
    while i < array.size(ob_zones)
        z = array.get(ob_zones, i)
        if bar_index - z.created_bar > ob_decay_bars
            // delete associated box if exists
            if i < array.size(ob_box_ids)
                bid = array.get(ob_box_ids, i)
                if bid != 0
                    box.delete(bid)
                array.remove(ob_box_ids, i)
            array.remove(ob_zones, i)
        else
            i += 1

f_add_or_merge_ob(top, bot, origin_bar, tf_idx) =>
    merged = false
    fuzz = ob_merge_fuzz
    for j = 0 to array.size(ob_zones) - 1
        z = array.get(ob_zones, j)
        if not na(z)
            // expanded overlap test with fuzz tolerance
            if not (bot > z.top * (1 + fuzz) or top < z.bot * (1 - fuzz))
                z.top := math.max(z.top, top)
                z.bot := math.min(z.bot, bot)
                z.origin_bar := math.min(z.origin_bar, origin_bar)
                z.tf_index := tf_idx
                z.last_seen_bar := bar_index
                array.set(ob_zones, j, z)
                merged := true
                break
    if not merged
        if array.size(ob_zones) < ob_max_zones
            nz = OBZone.new(top, bot, origin_bar, tf_idx, bar_index, bar_index)
            array.push(ob_zones, nz)
            array.push(ob_box_ids, 0)
        else
            // replace oldest (least recently seen)
            oldest_idx = 0
            oldest_age = -1
            for j = 0 to array.size(ob_zones) - 1
                z2 = array.get(ob_zones, j)
                age2 = bar_index - z2.last_seen_bar
                if age2 > oldest_age
                    oldest_age := age2
                    oldest_idx := j
            nz2 = OBZone.new(top, bot, origin_bar, tf_idx, bar_index, bar_index)
            // delete old box id if present
            if oldest_idx < array.size(ob_box_ids)
                old_id = array.get(ob_box_ids, oldest_idx)
                if old_id != 0
                    box.delete(old_id)
                array.set(ob_box_ids, oldest_idx, 0)
            array.set(ob_zones, oldest_idx, nz2)

// ==============================
// ORDER BLOCK DETECTION (cluster-based) — uses cached current and limited secured history
// NOTE: bounded historical security calls kept intentionally small; OB auto-disables if estimate exceeds budget
// ==============================
f_detect_ob_for_tf_cached(i) =>
    if not enable_orderblocks_cfg
        na
    else
        cluster_count = 0
        cluster_top = -1.0
        cluster_bot = 1e18
        tf = array.get(tfs, i)
        for k = 0 to ob_lookback - 1
            if k == 0
                is_mar_k = array.get(state_maru_geom, i)
                Et_k = array.get(state_Et, i)
                Et_mean_k = array.get(state_Et_sma, i)
                h_k = array.get(state_h, i)
                l_k = array.get(state_l, i)
            else
                // bounded historical checks (costly but limited by ob_lookback). This is the one remaining place of extra calls.
                is_mar_k = request.security(syminfo.tickerid, tf, f_is_marubozu_geom_o(open[k], high[k], low[k], close[k], body_ratio_th), lookahead=barmerge.lookahead_off, gaps=barmerge.gaps_off)
                Et_k = request.security(syminfo.tickerid, tf, f_energy_for_bar(open[k], high[k], low[k], close[k], volume[k]), lookahead=barmerge.lookahead_off, gaps=barmerge.gaps_off)
                Et_mean_k = request.security(syminfo.tickerid, tf, ta.sma(f_energy_for_bar(open, high, low, close, volume), lookback_mean), lookahead=barmerge.lookahead_off, gaps=barmerge.gaps_off)
                h_k = request.security(syminfo.tickerid, tf, high[k], lookahead=barmerge.lookahead_off, gaps=barmerge.gaps_off)
                l_k = request.security(syminfo.tickerid, tf, low[k], lookahead=barmerge.lookahead_off, gaps=barmerge.gaps_off)
            is_high_energy = not na(Et_k) and not na(Et_mean_k) and (Et_k > Et_mean_k)
            if is_mar_k or is_high_energy
                cluster_count += 1
                cluster_top := cluster_top < 0 ? h_k : math.max(cluster_top, h_k)
                cluster_bot := cluster_bot > 1e17 ? l_k : math.min(cluster_bot, l_k)
        if cluster_count >= ob_min_cluster
            padding = ob_zone_padding * close
            top = cluster_top + padding
            bot = cluster_bot - padding
            f_add_or_merge_ob(top, bot, bar_index - ob_lookback + 1, i)

// ==============================
// NORMALIZE / UTIL
// ==============================
normalize_by_atr(value, atr_val) =>
    atr_val == 0 ? value : value / (atr_val + eps)

// ==============================
// HISTORIES / DASH / EXPORT / STATE
// ==============================
var float[] hist_Etotal = array.new_float(tf_count, na)
var float[] hist_mean   = array.new_float(tf_count, na)
var int[]   hist_conf   = array.new_int(tf_count, 0)
var int[]   hist_dir    = array.new_int(tf_count, 0)
var float[] hist_phase  = array.new_float(tf_count, na)
var float[] phase_smooth = array.new_float(tf_count, na)
var float[] prev_momentum = array.new_float(tf_count, 0.0)

var table dash = table.new(position.top_right, tf_count + 1, 13, border_color=color.new(color.white, 80), frame_color=color.new(color.black, 90))
var table exp  = table.new(position.bottom_left, export_rows, 1)
var int last_major_label_bar = na

// ==============================
// MAIN FLOW (single cached fetch, canonical compute, one source of truth)
// ==============================
calls = f_fetch_and_cache_all()
enable_orderblocks = enable_orderblocks_cfg and (estimated_calls <= max_request_calls)

// canonical per-TF computation using cached state arrays (no additional request.security)
for i = 0 to tf_count - 1
    // read cached
    Et = array.get(state_Et, i)
    EtS = array.get(state_Et_sma, i)
    o_tf = array.get(state_o, i)
    h_tf = array.get(state_h, i)
    l_tf = array.get(state_l, i)
    c_tf = array.get(state_c, i)
    v_tf = array.get(state_v, i)
    vol_s = array.get(state_vol_sma, i)
    atr_tf = array.get(state_atr, i)
    is_maru = array.get(state_maru_geom, i)
    is_maru_v5 = array.get(state_maru_v5, i)
    prop59 = array.get(state_prop59, i)

    // combined metric normalized by ATR
    prop_term = na(prop59) ? 0.0 : (prop59 - 1.0)
    E_total_raw = na(Et) ? na : Et * (1.0 + JR * prop_term)
    E_total = na(E_total_raw) ? na : normalize_by_atr(E_total_raw, atr_tf)
    mean_past = na(EtS) ? na : normalize_by_atr(EtS, atr_tf)
    dev_ratio_total = na(E_total) or na(mean_past) ? 1.0 : math.abs(E_total - mean_past) / math.max(math.abs(mean_past), 1e-6)
    judgment_state = dev_ratio_total < JR
    direction_sign = na(E_total) or na(mean_past) ? 0.0 : E_total - mean_past
    judgment_dir = direction_sign > 0 ? 1 : direction_sign < 0 ? -1 : 0
    confidence_pct = int(math.round(100 * (1.0 - math.min(dev_ratio_total / JR, 1.0))))

    vol_gate = vol_s == 0 ? true : (v_tf >= min_vol_mult * vol_s)
    range_gate = atr_tf == 0 ? true : ((h_tf - l_tf) >= min_range_mult * atr_tf)
    is_maru_final = (is_maru or is_maru_v5) and vol_gate and range_gate
    final_sig = judgment_state and is_maru_final
    likely_dir = direction_sign > 0 ? 1 : direction_sign < 0 ? -1 : 0

    // write canonical computed fields
    array.set(state_E_total, i, E_total)
    array.set(state_mean_past, i, mean_past)
    array.set(state_conf, i, confidence_pct)
    array.set(state_dir, i, likely_dir)
    array.set(state_final, i, final_sig)

    // update hist arrays (for fusion history)
    if not na(E_total)
        array.set(hist_Etotal, i, E_total)
    if not na(mean_past)
        array.set(hist_mean, i, mean_past)
    array.set(hist_conf, i, confidence_pct)
    array.set(hist_dir, i, likely_dir)

    // OB detection (cached & bounded)
    if enable_orderblocks
        f_detect_ob_for_tf_cached(i)

// smoothing and phase update (resonant evolution)
alpha = 0.25
beta  = 0.15
phase_k = 0.4
freq_base = JR

for i = 0 to tf_count - 1
    Etv = array.get(hist_Etotal, i)
    Mv  = array.get(hist_mean, i)
    prevEt = nz(array.get(hist_Etotal, i), Etv)
    if not na(Etv) and not na(Mv)
        Et_smooth = alpha * Etv + (1 - alpha) * prevEt + beta * (prevEt - Mv)
        array.set(hist_Etotal, i, Et_smooth)

    Et_curr = array.get(hist_Etotal, i)
    Mprev = array.get(hist_mean, i)
    ph_prev = nz(array.get(hist_phase, i), 0.0)
    if not na(Et_curr) and not na(Mprev)
        dE = Et_curr - Mprev
        ph_new = math.fmod(ph_prev + freq_base * dE, 2 * math.pi)
        prev_sin = math.sin(ph_prev)
        prev_cos = math.cos(ph_prev)
        new_sin = math.sin(ph_new)
        new_cos = math.cos(ph_new)
        sin_s = phase_k * new_sin + (1 - phase_k) * prev_sin
        cos_s = phase_k * new_cos + (1 - phase_k) * prev_cos
        ph_smooth = math.atan2(sin_s, cos_s)
        array.set(hist_phase, i, ph_smooth)
        array.set(phase_smooth, i, ph_smooth)

// ==============================
// PHASE COHERENCE (WEIGHTED BY TF IMPORTANCE)
// longer TFs get naturally higher weight via log-based weighting
// ==============================
sum_sin_w = 0.0
sum_cos_w = 0.0
sum_w = 0.0
non_na_count = 0
for i = 0 to tf_count - 1
    ph = array.get(phase_smooth, i)
    if not na(ph)
        // weight: base + log-scale by index so higher TFs dominate slightly
        w = 1.0 + math.max(0.0, math.log(i + 2))
        sum_sin_w += math.sin(ph) * w
        sum_cos_w += math.cos(ph) * w
        sum_w += w
        non_na_count += 1
phase_coherence = sum_w == 0 ? 0.0 : math.sqrt(sum_sin_w * sum_sin_w + sum_cos_w * sum_cos_w) / sum_w

// slice segmentation (hysteresis + sustain)
var int slice_state = 0
var int slice_state_age = 0
if enable_slices
    slice_score = 0.0
    for i = 0 to tf_count - 1
        ph = array.get(phase_smooth, i)
        if not na(ph)
            vcos = math.cos(ph)
            slice_score += math.sign(vcos) * math.abs(vcos)
    slice_score := non_na_count == 0 ? 0.0 : slice_score / non_na_count
    if slice_state == 0
        if slice_score > slice_enter_threshold
            slice_state := 1; slice_state_age := 0
        else if slice_score < -slice_enter_threshold
            slice_state := -1; slice_state_age := 0
    else if slice_state == 1
        if slice_score < slice_exit_threshold
            slice_state := 0; slice_state_age := 0
        else
            slice_state_age += 1
    else if slice_state == -1
        if slice_score > -slice_exit_threshold
            slice_state := 0; slice_state_age := 0
        else
            slice_state_age += 1

// 4H cached boost flag (using canonical state arrays)
h4_boost_flag = false
if enable_4h_weight
    idx4 = 3
    f4Et = array.get(state_Et, idx4)
    f4EtS = array.get(state_Et_sma, idx4)
    v4 = array.get(state_v, idx4)
    vol4 = array.get(state_vol_sma, idx4)
    atr4 = array.get(state_atr, idx4)
    is_m4 = array.get(state_maru_geom, idx4)
    is_m4v5 = array.get(state_maru_v5, idx4)
    p4 = array.get(state_prop59, idx4)
    prop4 = na(p4) ? 0.0 : (p4 - 1.0)
    E4_total_raw = na(f4Et) ? na : f4Et * (1.0 + JR * prop4)
    E4_total = na(E4_total_raw) ? na : normalize_by_atr(E4_total_raw, atr4)
    mean4 = na(f4EtS) ? na : normalize_by_atr(f4EtS, atr4)
    dev4 = na(E4_total) or na(mean4) ? 1.0 : math.abs(E4_total - mean4) / math.max(math.abs(mean4), 1e-6)
    final4 = (dev4 < JR) and ((is_m4 or is_m4v5) and (v4 >= min_vol_mult * vol4) and ((array.get(state_h, idx4) - array.get(state_l, idx4)) >= min_range_mult * atr4))
    h4_boost_flag := final4

// aggregate prediction
long_strength = 0.0
short_strength = 0.0
for i = 0 to tf_count - 1
    Et_prev = array.get(hist_Etotal, i)
    M_prev  = array.get(hist_mean, i)
    dir_prev = array.get(state_dir, i)
    conf_prev = array.get(state_conf, i)
    if na(Et_prev) or na(M_prev)
        continue
    momentum = (Et_prev - M_prev) / (math.abs(M_prev) + 1e-6)
    prevm = nz(array.get(prev_momentum, i), 0.0)
    new_mom = momentum + 0.3 * prevm
    array.set(prev_momentum, i, new_mom)
    adj_conf = conf_prev * math.exp(-0.1)
    if enable_4h_weight and h4_boost_flag and i == 3
        adj_conf := adj_conf * h4_weight_mult
    if enable_slices and slice_state != 0
        slice_align = slice_state == math.sign(new_mom) ? 1.2 : 0.9
        adj_conf := adj_conf * slice_align
    next_dir = dir_prev * (new_mom == 0 ? 0 : math.sign(new_mom))
    if next_dir > 0
        long_strength += adj_conf
    else if next_dir < 0
        short_strength += adj_conf

// OB housekeeping (decay + clean)
if enable_orderblocks
    f_ob_decay_and_clean()

// Visuals & Labels (rate-limited)
major_label_placed = false
if phase_coherence > phase_coh_thresh
    if long_strength > short_strength and long_strength > 100 and (na(last_major_label_bar) or (bar_index - last_major_label_bar) >= label_rate_limit_bars)
        label.new(bar_index, high * 1.02, "Resonant Pred ▲", style=label.style_label_up, color=color.new(color.green, 0), textcolor=color.white)
        last_major_label_bar := bar_index
        major_label_placed := true
    else if short_strength > long_strength and short_strength > 100 and (na(last_major_label_bar) or (bar_index - last_major_label_bar) >= label_rate_limit_bars)
        label.new(bar_index, low * 0.98, "Resonant Pred ▼", style=label.style_label_down, color=color.new(color.red, 0), textcolor=color.white)
        last_major_label_bar := bar_index
        major_label_placed := true

// per-TF small labels (cleared each bar)
var label[] tf_labels = array.new<label>()
for j = 0 to array.size(tf_labels) - 1
    label.delete(array.get(tf_labels, j))
array.clear(tf_labels)

long_votes = 0
short_votes = 0

for i = 0 to tf_count - 1
    final_flag = array.get(state_final, i)
    conf = array.get(state_conf, i)
    dirv = array.get(state_dir, i)
    ph = array.get(phase_smooth, i)
    // draw small labels
    if final_flag
        lab = dirv == 1 ? label.new(bar_index, array.get(state_h, i), text=array.get(tf_names, i) + " ▲ " + str.tostring(conf) + "%", style=label.style_label_up, color=color.new(color.green, 0), textcolor=color.white) :
                          label.new(bar_index, array.get(state_l, i), text=array.get(tf_names, i) + " ▼ " + str.tostring(conf) + "%", style=label.style_label_down, color=color.new(color.red, 0), textcolor=color.white)
        array.push(tf_labels, lab)
    else
        p = array.get(state_prop59, i)
        if not na(p) and p > 1.0
            labp = dirv == 1 ? label.new(bar_index, array.get(state_h, i) * 1.005, text=array.get(tf_names, i) + " P▲ " + str.tostring(math.round(p * 1000)/1000), style=label.style_label_up, color=color.new(color.lime, 0), textcolor=color.white) :
                   dirv == -1 ? label.new(bar_index, array.get(state_l, i) * 0.995, text=array.get(tf_names, i) + " P▼ " + str.tostring(math.round(p * 1000)/1000), style=label.style_label_down, color=color.new(color.orange, 0), textcolor=color.white) : na
            if not na(labp)
                array.push(tf_labels, labp)
    if dirv == 1
        long_votes += 1
    else if dirv == -1
        short_votes += 1

if not major_label_placed
    if long_votes >= agree_threshold and (na(last_major_label_bar) or (bar_index - last_major_label_bar) >= label_rate_limit_bars)
        label.new(bar_index, high * 1.02, "Next ▲", style=label.style_label_up, color=color.new(color.green, 0), textcolor=color.white)
        last_major_label_bar := bar_index
    else if short_votes >= agree_threshold and (na(last_major_label_bar) or (bar_index - last_major_label_bar) >= label_rate_limit_bars)
        label.new(bar_index, low * 0.98, "Next ▼", style=label.style_label_down, color=color.new(color.red, 0), textcolor=color.white)
        last_major_label_bar := bar_index

// Dashboard header
table.cell(dash, 0, 0, "TF", bgcolor=color.new(color.blue, 0), text_color=color.white)
table.cell(dash, 0, 1, "Signal", bgcolor=color.new(color.blue, 0), text_color=color.white)
table.cell(dash, 0, 2, "Dir", bgcolor=color.new(color.blue, 0), text_color=color.white)
table.cell(dash, 0, 3, "Conf", bgcolor=color.new(color.blue, 0), text_color=color.white)
table.cell(dash, 0, 4, "E_total / Mean", bgcolor=color.new(color.blue, 0), text_color=color.white)
table.cell(dash, 0, 5, "LastTime", bgcolor=color.new(color.blue, 0), text_color=color.white)
table.cell(dash, 0, 6, "Count", bgcolor=color.new(color.blue, 0), text_color=color.white)
table.cell(dash, 0, 7, "Next", bgcolor=color.new(color.blue, 0), text_color=color.white)
table.cell(dash, 0, 8, "Maru?", bgcolor=color.new(color.blue, 0), text_color=color.white)
table.cell(dash, 0, 9, "Prop59", bgcolor=color.new(color.blue, 0), text_color=color.white)
table.cell(dash, 0, 10, "OB", bgcolor=color.new(color.blue, 0), text_color=color.white)
table.cell(dash, 0, 11, "Slice", bgcolor=color.new(color.blue, 0), text_color=color.white)
table.cell(dash, 0, 12, "PhaseC", bgcolor=color.new(color.blue, 0), text_color=color.white)

// Dashboard rows (single canonical reads)
for i = 0 to tf_count - 1
    tf_name = array.get(tf_names, i)
    Etv = array.get(state_E_total, i)
    Mv  = array.get(state_mean_past, i)
    confv = array.get(state_conf, i)
    dirv = array.get(state_dir, i)
    is_m_f = array.get(state_maru_geom, i)
    is_mv5 = array.get(state_maru_v5, i)
    propf = array.get(state_prop59, i)
    final_flag = array.get(state_final, i)
    bg_col = final_flag ? (dirv == 1 ? color.new(color.green, 0) : color.new(color.red, 0)) : (is_m_f ? color.new(color.orange, 10) : color.new(color.gray, 80))
    ptxt = na(propf) ? "n/a" : str.tostring(math.round(propf * 1000) / 1000)
    next_col = dirv == 1 ? "▲" : dirv == -1 ? "▼" : "—"
    maru_col = is_m_f or is_mv5 ? "Y" : "N"
    // quick OB indicator: any zone overlaps current price
    ob_col = "—"
    for zi = 0 to array.size(ob_zones) - 1
        if zi >= array.size(ob_zones)
            break
        z = array.get(ob_zones, zi)
        if not na(z)
            if (close <= z.top) and (close >= z.bot)
                ob_col := "Z"
                break
    slice_col = enable_slices ? (slice_state == 1 ? "UP" : slice_state == -1 ? "DN" : "NEU") : "—"
    table.cell(dash, i + 1, 0, tf_name, bgcolor=bg_col, text_color=color.white)
    table.cell(dash, i + 1, 1, final_flag ? "JR+Maru" : (is_m_f ? "Marubozu" : "—"), bgcolor=bg_col, text_color=color.white)
    table.cell(dash, i + 1, 2, dirv == 1 ? "BULL" : dirv == -1 ? "BEAR" : "NEU", bgcolor=bg_col, text_color=color.white)
    table.cell(dash, i + 1, 3, str.tostring(confv) + "%", bgcolor=bg_col, text_color=color.white)
    table.cell(dash, i + 1, 4, str.tostring(na(Etv) ? "na" : math.round(Etv * 1000000) / 1000000) + " / " + str.tostring(na(Mv) ? "na" : math.round(Mv * 1000000) / 1000000), bgcolor=bg_col, text_color=color.white)
    table.cell(dash, i + 1, 5, str.tostring(time, "yyyy-MM-dd HH:mm"), bgcolor=bg_col, text_color=color.white)
    table.cell(dash, i + 1, 6, "—", bgcolor=bg_col, text_color=color.white)
    table.cell(dash, i + 1, 7, next_col, bgcolor=bg_col, text_color=color.white)
    table.cell(dash, i + 1, 8, maru_col, bgcolor=bg_col, text_color=color.white)
    table.cell(dash, i + 1, 9, ptxt, bgcolor=bg_col, text_color=color.white)
    table.cell(dash, i + 1, 10, ob_col, bgcolor=bg_col, text_color=color.white)
    table.cell(dash, i + 1, 11, slice_col, bgcolor=bg_col, text_color=color.white)
    table.cell(dash, i + 1, 12, str.tostring(math.round(phase_coherence * 1000)/1000), bgcolor=bg_col, text_color=color.white)

// Export rows
row_index = 0
for i = 0 to tf_count - 1
    if row_index >= export_rows
        break
    txt = str.tostring(time, "yyyy-MM-dd HH:mm:ss") + "," + array.get(tf_names, i) + "," + (na(array.get(state_E_total, i)) ? "na" : str.tostring(math.round(array.get(state_E_total, i) * 1000000)/1000000)) + "," + (na(array.get(state_mean_past, i)) ? "na" : str.tostring(math.round(array.get(state_mean_past, i) * 1000000)/1000000)) + "," + (na(array.get(state_prop59, i)) ? "n/a" : str.tostring(math.round(array.get(state_prop59, i) * 1000)/1000)) + "," + str.tostring(array.get(state_conf, i)) + "," + (array.get(state_dir, i) == 1 ? "1" : array.get(state_dir, i) == -1 ? "-1" : "0") + "," + str.tostring(math.round(phase_coherence * 1000)/1000)
    table.cell(exp, row_index, 0, txt)
    row_index += 1

// Draw OB boxes only when zone added/changed (reuse pool)
if enable_orderblocks
    for zi = 0 to array.size(ob_zones) - 1
        if zi >= ob_max_zones
            break
        z = array.get(ob_zones, zi)
        if not na(z)
            // reuse box id slot
            bid = zi < array.size(ob_box_ids) ? array.get(ob_box_ids, zi) : 0
            // delete previous to avoid duplicates then create new
            if bid != 0
                box.delete(bid)
                array.set(ob_box_ids, zi, 0)
            b = box.new(x1=z.origin_bar, y1=z.top, x2=bar_index, y2=z.bot, border_color=color.new(color.purple, 60), bgcolor=color.new(color.purple, 92), extend=extend.right)
            if zi < array.size(ob_box_ids)
                array.set(ob_box_ids, zi, b)
            else
                array.push(ob_box_ids, b)

// Plot coherence & shade
if plot_coherence
    plot(phase_coherence, "Quantum Coherence", color=color.new(color.aqua, 0), linewidth=2)
if shade_bg
    bgcolor(color.new(color.lime, 90 - int(phase_coherence * 90)))

// Alerts (use alertcondition)
for i = 0 to tf_count - 1
    final_flag = array.get(state_final, i)
    alertcondition(final_flag, title="JR+Marubozu " + array.get(tf_names, i), message="JR+Marubozu on " + array.get(tf_names, i) + " | Time: " + str.tostring(time, "yyyy-MM-dd HH:mm"))

// consensus alerts
alertcondition(long_votes >= agree_threshold, title="Next-Candle Likely ▲", message="Multi-TF next-candle likely UP")
alertcondition(short_votes >= agree_threshold, title="Next-Candle Likely ▼", message="Multi-TF next-candle likely DOWN")

// Optimizer (last-bar only)
if opt_run_optimizer and barstate.islast
    min_hf = math.max(0.1, hidden_factor_default * 0.5)
    max_hf = math.min(1.0, hidden_factor_default * 1.5)
    step = (max_hf - min_hf) / math.max(1, opt_grid_steps - 1)
    best_hf := hidden_factor_default
    best_score := -1.0
    mv4h = request.security(syminfo.tickerid, "240", math.abs(close - close[1]), lookahead=barmerge.lookahead_off, gaps=barmerge.gaps_off)
    agg_res = 0.0
    for j = 0 to tf_count - 1
        agg_res += nz(array.get(hist_Etotal, j), 0.0)
    for s = 0 to opt_grid_steps - 1
        cand = min_hf + step * s
        scaled = agg_res * cand
        corr = ta.correlation(scaled == 0 ? 0 : scaled, mv4h, opt_eval_bars)
        score = math.abs(corr)
        if score > best_score
            best_score := score
            best_hf := cand
    label.new(bar_index, high, "Opt HF: " + str.tostring(best_hf, "#.000") + " score:" + str.tostring(math.round(best_score*1000)/1000), style=label.style_label_left, color=color.new(color.yellow, 0), textcolor=color.black)

// Final status label (shows estimated security calls and OB auto-disable)
info_txt = "EstCalls:" + str.tostring(estimated_calls) + " / Max:" + str.tostring(max_request_calls) + " | OB:" + (enable_orderblocks ? "ON" : "OFF")
label.new(bar_index, high * 1.01, info_txt, style=label.style_label_right, color=color.new(color.gray, 80), textcolor=color.white)
