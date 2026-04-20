from typing import List, Optional, Any, Tuple
from src.publish.definitions.columns import TAB_COLUMNS
from src.publish.core.formatting import ROW_INDEXES, format_section_header, format_stat_value, format_height
from src.publish.definitions.config import (SECTIONS_CONFIG, SUBSECTIONS, SHEET_FORMATTING, STAT_RATES, DEFAULT_STAT_RATE, SUMMARY_THRESHOLDS, ColumnContext)
from src.publish.core.calculations import get_percentile_rank, evaluate_formula, calculate_entity_stats, evaluate_expression
from src.publish.core.layout import _base_section, _format_companion

# ============================================================================
# ROW BUILDING
# ============================================================================

def build_entity_row(entity_data: dict, columns_list: List[Tuple],
                     percentiles: dict, entity_type: str = 'player',
                     mode: str = 'per_possession', seasons_str: str = '',
                     row_section: Optional[str] = None,
                     section_data: Optional[dict] = None,
                     context: Optional[dict] = None) -> list:
    """
    Build a single data row for any entity type.

    Evaluates all formulas, applies scaling, calculates percentile rank,
    and formats values. 100% config-driven.

    Supports TWO modes:
    1. Legacy single-section mode (row_section set):
       Uses entity_data + percentiles for matching section, blanks others.
    2. Merged multi-section mode (section_data set):
       section_data = {section_name: (entity_data, percentiles, seasons_str)}
       Fills each stats-section column from its corresponding data.
       Non-stats columns use the first available entity_data.
    """
    if section_data:
        # Merged mode — pre-calculate stats per section (supports ColumnContext keys)
        calculated_by_section = {}
        for sec_name, (sec_entity, sec_pcts, sec_seasons) in section_data.items():
            if hasattr(sec_name, 'rate') and getattr(sec_name, 'rate'):
                sec_mode = sec_name.rate
            else:
                sec_mode = mode
            sec_ctx = dict(context or {})
            if hasattr(sec_name, 'base_section'):
                is_current = sec_name.base_section == 'current_stats'
            else:
                is_current = str(sec_name).startswith('current_stats')
            if is_current:
                sec_ctx['seasons_in_query'] = 1
            calculated_by_section[sec_name] = calculate_entity_stats(
                sec_entity, entity_type, sec_mode, sec_ctx
            )
        # For non-stats columns, use the first section's entity data
        first_section = next(iter(section_data))
        primary_entity = section_data[first_section][0]
        primary_calculated = calculated_by_section[first_section]
    else:
        # Legacy single-section mode
        primary_entity = entity_data
        sec_ctx = dict(context or {})
        
        if hasattr(row_section, 'base_section'):
            is_current = row_section.base_section == 'current_stats'
        else:
            is_current = row_section and str(row_section).startswith('current_stats')

        if is_current:
            sec_ctx['seasons_in_query'] = 1
        primary_calculated = calculate_entity_stats(entity_data, entity_type, mode, sec_ctx)

    row = []

    for entry in columns_list:
        col_key, col_def = entry[0], entry[1]
        col_ctx = entry[3] if len(entry) > 3 else None
        is_pct = col_def.get('is_generated_percentile', False)

        if col_def.get('is_separator'):
            row.append('')
            continue

        col_ctx_cfg = SECTIONS_CONFIG.get(_base_section(col_ctx), {})
        is_stats_section = col_ctx_cfg.get('stats_timeframe')

        if section_data and is_stats_section:
            # Pick the right data for this section
            if col_ctx in section_data:
                sec_entity, sec_pcts, _ = section_data[col_ctx]
                calculated = calculated_by_section[col_ctx]
                pcts = sec_pcts
            else:
                row.append('')
                continue
        elif row_section and is_stats_section and _base_section(col_ctx) != _base_section(row_section):
            # Legacy mode — blank out wrong-section columns
            row.append('')
            continue
        else:
            # Non-stats column or matching section
            calculated = primary_calculated if not section_data else primary_calculated
            pcts = percentiles if not section_data else (
                section_data[first_section][1] if section_data else percentiles
            )
            sec_entity = primary_entity

        if is_pct:
            base_key = col_def.get('base_stat', col_key.replace('_pct', ''))
            base_def = TAB_COLUMNS.get(base_key, {})
            value = calculated.get(base_key)

            if value is not None and isinstance(value, (int, float)) and base_key in pcts:
                reverse = base_def.get('percentile') == 'reverse'
                rank = get_percentile_rank(value, pcts[base_key], reverse)
                median = _get_value_at_percentile(pcts[base_key], 50, reverse=False)
                diff = value - median if median is not None else None
                row.append(_format_companion(rank, diff, base_def))
            else:
                row.append('')
            continue

        # Non-percentile column
        # Non-stats section column — evaluate formula and format
        if not col_ctx_cfg.get('stats_timeframe'):
            use_entity = sec_entity if section_data and is_stats_section else primary_entity
            _col_mode = col_ctx.rate if isinstance(col_ctx, ColumnContext) and col_ctx.rate else mode
            value = evaluate_formula(col_key, use_entity, entity_type, _col_mode, context)
            if value is None:
                row.append('')
            elif col_def.get('format') == 'measurement':
                row.append(format_height(value))
            elif col_def.get('percentile') is not None:
                formatted = format_stat_value(value, col_def)
                row.append(formatted if formatted is not None else '')
            else:
                row.append(value)
            continue

        # Dynamically-generated opponent column (Teams sheet) — eval directly
        if col_def.get('is_opponent_col'):
            opp_expr = col_def.get('values', {}).get('team')
            value = evaluate_expression(opp_expr, sec_entity)
            _col_mode = col_ctx.rate if isinstance(col_ctx, ColumnContext) and col_ctx.rate else mode
            override = col_def.get('mode_overrides', {}).get(_col_mode)
            active_def = override if override else col_def
            formatted = format_stat_value(value, active_def)
            row.append(formatted if formatted is not None else '')
            continue

        # Stat column — use pre-calculated value
        value = calculated.get(col_key)
        _col_mode = col_ctx.rate if isinstance(col_ctx, ColumnContext) and col_ctx.rate else mode
        override = col_def.get('mode_overrides', {}).get(_col_mode)
        active_def = override if override else col_def
        formatted = format_stat_value(value, active_def)
        row.append(formatted if formatted is not None else '')

    return row

# ============================
# MOVED FROM FORMATTING.PY
# ============================
def build_merged_entity_row(player_id, columns_list: List[Tuple],
                            current_data: Optional[dict],
                            historical_data: Optional[dict],
                            postseason_data: Optional[dict],
                            pct_by_rate: dict,
                            entity_type: str = 'player',
                            historical_timeframe: str = '', post_seasons: str = '',
                            opp_percentiles: Optional[dict] = None,
                            context: Optional[dict] = None) -> Tuple[list, List[dict], List[dict]]:
    """
    Build a single merged data row with current + historical + postseason stats.

    All 3 stat rates are written simultaneously via composite section keys
    (e.g. 'current_stats__per_possession'). Rate switching is handled by
    column visibility in the spreadsheet.

    pct_by_rate: {rate: {base_section: {col_key: sorted_values}}}
    opp_percentiles: {col_key: {composite_section: sorted_vals}}
    """
    section_data = {}
    for rate_name in STAT_RATES:
        rate_pcts = pct_by_rate.get(rate_name, {})
        
        # Handle current stats
        if current_data:
            section_data[ColumnContext(base_section='current_stats', rate=rate_name)] = (current_data, rate_pcts.get('current_stats', {}), '')
            
        # Handle historical stats (dict mapped by supported years)
        if historical_data:
            if isinstance(historical_data, dict):
                for y, h_data in historical_data.items():
                    if h_data:
                        section_data[ColumnContext(base_section='historical_stats', timeframe=int(y), rate=rate_name)] = (
                            h_data, rate_pcts.get(f'historical_stats_{y}yr', {}), str(y)
                        )
            else:
                section_data[ColumnContext(base_section='historical_stats', rate=rate_name)] = (
                    historical_data, rate_pcts.get('historical_stats', {}), historical_timeframe
                )
                
        # Handle postseason stats (dict mapped by supported years)
        if postseason_data:
            if isinstance(postseason_data, dict):
                for y, p_data in postseason_data.items():
                    if p_data:
                        section_data[ColumnContext(base_section='postseason_stats', timeframe=int(y), rate=rate_name)] = (
                            p_data, rate_pcts.get(f'postseason_stats_{y}yr', {}), str(y)
                        )
            else:
                section_data[ColumnContext(base_section='postseason_stats', rate=rate_name)] = (
                    postseason_data, rate_pcts.get('postseason_stats', {}), post_seasons
                )

    if current_data:
        primary_entity = current_data
    elif isinstance(historical_data, dict) and any(historical_data.values()):
        primary_entity = next(d for d in historical_data.values() if d)
    elif isinstance(postseason_data, dict) and any(postseason_data.values()):
        primary_entity = next(d for d in postseason_data.values() if d)
    else:
        primary_entity = historical_data or postseason_data or {}

    row = build_entity_row(
        primary_entity, columns_list, {},
        entity_type=entity_type,
        section_data=section_data,
        context=context,
    )

    # Collect percentile info for companion column shading.
    percentile_cells = []
    link_cells = []
    for col_idx, entry in enumerate(columns_list):
        col_key, col_def = entry[0], entry[1]
        
        # Link routing
        fmt = col_def.get('format')
        if fmt == 'team_link' and context and 'team_gids' in context:
            team_gids = context['team_gids']
            if entity_type == 'player':
                abbr = primary_entity.get('team_abbr') or primary_entity.get('abbr')
            else:
                # all_teams
                abbr = primary_entity.get('abbr')
                
            if abbr and abbr in team_gids:
                link_cells.append({
                    'col': col_idx,
                    'uri': f"#gid={team_gids[abbr]}"
                })
        
        col_ctx = entry[3] if len(entry) > 3 else None
        is_pct = col_def.get('is_generated_percentile', False)

        if not is_pct:
            continue

        col_ctx_cfg = SECTIONS_CONFIG.get(_base_section(col_ctx), {})
        is_stats_section = col_ctx_cfg.get('stats_timeframe')
        base_key = col_def.get('base_stat', col_key.replace('_pct', ''))

        if is_stats_section:
            if col_ctx not in section_data:
                continue
            sec_entity, sec_pcts, _ = section_data[col_ctx]
            if isinstance(col_ctx, ColumnContext):
                sec_mode = col_ctx.rate if col_ctx.rate else DEFAULT_STAT_RATE
            else:
                sec_mode = col_ctx.split('__')[1] if col_ctx and '__' in str(col_ctx) else DEFAULT_STAT_RATE

            # Opponent companion: compute value from base opponent formula
            if col_def.get('is_opponent_col') and opp_percentiles:
                opp_pop = opp_percentiles.get(base_key, {}).get(_base_section(col_ctx))
                if opp_pop is not None:
                    base_col_def = None
                    for e2 in columns_list:
                        if e2[0] == base_key:
                            base_col_def = e2[1]
                            break
                    if base_col_def:
                        opp_expr = base_col_def.get('values', {}).get('team')
                        value = evaluate_expression(opp_expr, sec_entity)
                        if value is not None:
                            reverse = base_col_def.get('percentile') == 'reverse'
                            rank = get_percentile_rank(value, opp_pop, reverse)
                            median = _get_value_at_percentile(opp_pop, 50, reverse=False)
                            diff = value - median if median is not None else None
                            row[col_idx] = _format_companion(rank, diff, base_col_def)
                            percentile_cells.append({
                                'col': col_idx,
                                'percentile': rank,
                                'reverse': False,
                            })
                            # Also color the base opponent stat cell
                            if col_idx > 0:
                                percentile_cells.append({
                                    'col': col_idx - 1,
                                    'percentile': rank,
                                    'reverse': False,
                                })
                continue

            # Regular companion
            base_def = TAB_COLUMNS.get(base_key, col_def)
            sec_ctx_pct = dict(context or {})
            if hasattr(col_ctx, 'base_section'):
                is_current = col_ctx.base_section == 'current_stats'
            else:
                is_current = str(col_ctx).startswith('current_stats')

            if is_current:
                sec_ctx_pct['seasons_in_query'] = 1
            calculated = calculate_entity_stats(sec_entity, entity_type, sec_mode, sec_ctx_pct)
            value = calculated.get(base_key)

            if value is not None and base_key in sec_pcts:
                reverse = base_def.get('percentile') == 'reverse'
                rank = get_percentile_rank(value, sec_pcts[base_key], reverse)
                percentile_cells.append({
                    'col': col_idx,
                    'percentile': rank,
                    'reverse': reverse,
                })
                # Also color the base stat value cell
                if col_idx > 0:
                    percentile_cells.append({
                        'col': col_idx - 1,
                        'percentile': rank,
                        'reverse': reverse,
                    })
        else:
            # Non-stats section — use default mode's current_stats percentiles
            default_current_key = ColumnContext(base_section='current_stats', rate=DEFAULT_STAT_RATE)
            if default_current_key in section_data:
                sec_entity, sec_pcts, _ = section_data[default_current_key]
            elif section_data:
                first_key = next(iter(section_data))
                sec_entity, sec_pcts, _ = section_data[first_key]
            else:
                continue
            base_def = TAB_COLUMNS.get(base_key, col_def)
            calculated = calculate_entity_stats(sec_entity, entity_type, DEFAULT_STAT_RATE, context)
            value = calculated.get(base_key)

            if value is not None and base_key in sec_pcts:
                reverse = base_def.get('percentile') == 'reverse'
                rank = get_percentile_rank(value, sec_pcts[base_key], reverse)
                percentile_cells.append({
                    'col': col_idx,
                    'percentile': rank,
                    'reverse': reverse,
                })
                # Also color the base stat value cell
                if col_idx > 0:
                    percentile_cells.append({
                        'col': col_idx - 1,
                        'percentile': rank,
                        'reverse': reverse,
                    })

    return row, percentile_cells, link_cells


def _get_value_at_percentile(sorted_values: List, percentile: float,
                             reverse: bool = False) -> Any:
    """Get the interpolated value at a given percentile (0-100) from sorted values.

    Supports both plain sorted lists and weighted (value, weight) tuples
    from calculate_all_percentiles.
    """
    if not sorted_values:
        return None

    # Detect weighted tuples vs plain values
    is_weighted = isinstance(sorted_values[0], (tuple, list))

    if is_weighted:
        values = [entry[0] for entry in sorted_values]
        weights = [entry[1] for entry in sorted_values]
    else:
        values = sorted_values
        weights = None

    n = len(values)
    if n == 1:
        return values[0]

    # For reverse columns (lower = better), Best (100) -> lowest value
    if reverse:
        percentile = 100 - percentile

    if weights:
        # Weighted interpolation: find the value at the given percentile
        # using cumulative weight distribution
        total_weight = sum(weights)
        if total_weight <= 0:
            return None
        target = (percentile / 100.0) * total_weight
        cumulative = 0.0
        for i, (val, w) in enumerate(sorted_values):
            cumulative += w
            if cumulative >= target:
                return val
        return values[-1]
    else:
        idx = percentile / 100 * (n - 1)
        lower = int(idx)
        upper = min(lower + 1, n - 1)
        frac = idx - lower
        v_lower = values[lower]
        v_upper = values[upper]
        if not isinstance(v_lower, (int, float)) or not isinstance(v_upper, (int, float)):
            return None
        return v_lower * (1 - frac) + v_upper * frac


def build_summary_rows(columns_list: List[Tuple],
                       percentile_pops: dict,
                       mode: str = 'per_possession',
                       opp_percentiles: Optional[dict] = None) -> Tuple[List[list], List[dict]]:
    """
    Build summary rows (Best, 75th, Average, 25th, Worst) for Teams/Players sheets.

    For each stat column, looks up the value at that percentile threshold.
    Non-stat columns are left blank except 'names' which gets the label.
    Generated percentile columns show the percentile level itself.

    Returns:
        (rows, percentile_cells) where rows is list of 5 row lists,
        and percentile_cells is list of {row, col, percentile} dicts
        (row index is relative — caller must add data_start offset).
    """
    rows = []
    pct_cells = []

    for label, pct_level in SUMMARY_THRESHOLDS:
        row = []
        for col_idx, entry in enumerate(columns_list):
            col_key, col_def = entry[0], entry[1]
            col_ctx = entry[3] if len(entry) > 3 else None

            if col_def.get('is_separator'):
                row.append('')
                continue

            # Name column gets the label
            if col_key == 'name':
                row.append(label)
                continue

            # Generated percentile columns show rank + over/under
            if col_def.get('is_generated_percentile', False):
                base_key = col_def.get('base_stat', col_key.replace('_pct', ''))
                base_def = TAB_COLUMNS.get(base_key, col_def)
                col_ctx_cfg = SECTIONS_CONFIG.get(_base_section(col_ctx), {})
                is_stats_section = col_ctx_cfg.get('stats_timeframe')

                value = None
                median = None

                if col_def.get('is_opponent_col') and opp_percentiles:
                    if is_stats_section and col_ctx:
                        opp_pop = opp_percentiles.get(base_key, {}).get(_base_section(col_ctx))
                        if opp_pop:
                            reverse = base_def.get('percentile') == 'reverse'
                            value = _get_value_at_percentile(opp_pop, pct_level, reverse)
                            median = _get_value_at_percentile(opp_pop, 50, False)
                elif is_stats_section:
                    pop_key = f'{col_ctx}:{base_key}'
                    sorted_vals = percentile_pops.get(pop_key,
                                  percentile_pops.get(base_key))
                    if sorted_vals:
                        reverse = base_def.get('percentile') == 'reverse'
                        value = _get_value_at_percentile(sorted_vals, pct_level, reverse)
                        median = _get_value_at_percentile(sorted_vals, 50, reverse=False)
                else:
                    sorted_vals = percentile_pops.get(base_key)
                    if sorted_vals:
                        reverse = base_def.get('percentile') == 'reverse'
                        value = _get_value_at_percentile(sorted_vals, pct_level, reverse)
                        median = _get_value_at_percentile(sorted_vals, 50, reverse=False)

                diff = value - median if value is not None and median is not None else None
                row.append(_format_companion(pct_level, diff, base_def))
                pct_cells.append({
                    'col': col_idx,
                    'percentile': pct_level,
                    'reverse': False,
                    'row_offset': len(rows),
                })
                continue

            # Non-stat, non-percentile columns are blank
            if (not col_def.get('percentile')
                    and not col_def.get('is_generated_percentile', False)):
                row.append('')
                continue

            # Opponent columns: use opp_percentiles populations
            if col_def.get('is_opponent_col') and opp_percentiles:
                col_ctx_cfg = SECTIONS_CONFIG.get(_base_section(col_ctx), {})
                if col_ctx_cfg.get('stats_timeframe') and col_ctx:
                    opp_pop = opp_percentiles.get(col_key, {}).get(_base_section(col_ctx))
                    if opp_pop:
                        reverse = col_def.get('percentile') == 'reverse'
                        val = _get_value_at_percentile(opp_pop, pct_level, reverse)
                        if val is not None:
                            formatted = format_stat_value(val, col_def)
                            row.append(formatted if formatted is not None else '')
                            pct_cells.append({
                                'col': col_idx,
                                'percentile': pct_level,
                                'reverse': False,
                                'row_offset': len(rows),
                            })
                            continue
                row.append('')
                continue

            # Regular stat columns: look up in section-specific populations
            col_ctx_cfg = SECTIONS_CONFIG.get(_base_section(col_ctx), {})
            is_stats_section = col_ctx_cfg.get('stats_timeframe')
            pop_key = f'{col_ctx}:{col_key}'

            # Stats-section columns: look up via section:col_key
            if is_stats_section and (
                    pop_key in percentile_pops or col_key in percentile_pops):
                sorted_vals = percentile_pops.get(pop_key,
                              percentile_pops.get(col_key))
                if sorted_vals:
                    reverse = col_def.get('percentile') == 'reverse'
                    val = _get_value_at_percentile(sorted_vals, pct_level, reverse)
                    if val is not None:
                        formatted = format_stat_value(val, col_def)
                        row.append(formatted if formatted is not None else '')
                        pct_cells.append({
                            'col': col_idx,
                            'percentile': pct_level,
                            'reverse': False,
                            'row_offset': len(rows),
                        })
                        continue

            # Non-stats columns with percentile (profile): direct col_key lookup
            if not is_stats_section and col_def.get('percentile'):
                sorted_vals = percentile_pops.get(col_key)
                if sorted_vals:
                    reverse = col_def.get('percentile') == 'reverse'
                    val = _get_value_at_percentile(sorted_vals, pct_level, reverse)
                    if val is not None:
                        if col_def.get('format') == 'measurement':
                            formatted = format_height(val)
                        else:
                            formatted = format_stat_value(val, col_def)
                        row.append(formatted if formatted is not None else '')
                        pct_cells.append({
                            'col': col_idx,
                            'percentile': pct_level,
                            'reverse': False,
                            'row_offset': len(rows),
                        })
                        continue

            row.append('')

        rows.append(row)

    return rows, pct_cells