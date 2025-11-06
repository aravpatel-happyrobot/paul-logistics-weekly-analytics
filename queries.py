def carrier_asked_transfer_over_total_transfer_attempt_stats_query(date_filter: str, org_id: str, PEPSI_BROKER_NODE_ID: str) -> str:
    # percentage of carrier asked transfers over total transfer attempts
    return f"""
            WITH recent_runs AS (
                SELECT id AS run_id
                FROM public_runs
                WHERE {date_filter}
            ),
            sessions AS (
                SELECT run_id, user_number FROM public_sessions
                WHERE {date_filter}
                AND org_id = '{org_id}'
            ),
            transfer_stats AS (
                SELECT
                    JSONExtractString(no.flat_data, 'result.transfer.transfer_reason') AS transfer_reason,
                    COUNT(*) AS count
                FROM public_node_outputs no
                INNER JOIN recent_runs rr ON no.run_id = rr.run_id
                INNER JOIN public_nodes n ON no.node_id = n.id
                INNER JOIN sessions s ON no.run_id = s.run_id
                WHERE n.org_id = '{org_id}'
                  AND no.node_persistent_id = '{PEPSI_BROKER_NODE_ID}'
                  AND JSONHas(no.flat_data, 'result.transfer.transfer_reason') = 1
                  AND JSONExtractString(no.flat_data, 'result.transfer.transfer_reason') != ''
                  AND JSONExtractString(no.flat_data, 'result.transfer.transfer_reason') != 'null'
                  AND upper(JSONExtractString(no.flat_data, 'result.transfer.transfer_reason')) != 'NO_TRANSFER_INVOLVED'
                  AND JSONHas(no.flat_data, 'result.transfer.transfer_attempt') = 1
                  AND upper(JSONExtractString(no.flat_data, 'result.transfer.transfer_attempt')) = 'YES'
                  AND s.user_number != '+19259898099'
                GROUP BY transfer_reason
            ),
            org_totals AS (
                SELECT SUM(count) AS total_transfers
                FROM transfer_stats
            ),
            carrier_asked_stats AS (
                SELECT
                    ts.count AS carrier_asked_count,
                    ot.total_transfers AS total_transfer_attempts,
                    ROUND((ts.count * 100.0) / ot.total_transfers, 2) AS carrier_asked_percentage
                FROM transfer_stats ts
                CROSS JOIN org_totals ot
                WHERE ts.transfer_reason = 'CARRIER_ASKED_FOR_TRANSFER'
            )
            SELECT
                carrier_asked_count,
                total_transfer_attempts,
                carrier_asked_percentage
            FROM carrier_asked_stats
            LIMIT 1
        """

def carrier_asked_transfer_over_total_call_attempts_stats_query(date_filter: str, org_id: str, PEPSI_BROKER_NODE_ID: str) -> str:
    # percentage of carrier asked transfers over total call attempts
    return f"""
            WITH recent_runs AS (
                SELECT id AS run_id
                FROM public_runs
                WHERE {date_filter}
            ),
            sessions AS (
                SELECT run_id, user_number FROM public_sessions
                WHERE {date_filter}
                AND org_id = '{org_id}'
            ),
            transfer_stats AS (
                SELECT
                    JSONExtractString(no.flat_data, 'result.transfer.transfer_reason') AS transfer_reason,
                    COUNT(*) AS count
                FROM public_node_outputs no
                INNER JOIN recent_runs rr ON no.run_id = rr.run_id
                INNER JOIN public_nodes n ON no.node_id = n.id
                INNER JOIN sessions s ON no.run_id = s.run_id
                WHERE n.org_id = '{org_id}'
                  AND no.node_persistent_id = '{PEPSI_BROKER_NODE_ID}'
                  AND JSONHas(no.flat_data, 'result.transfer.transfer_reason') = 1
                  AND JSONExtractString(no.flat_data, 'result.transfer.transfer_reason') != ''
                  AND JSONExtractString(no.flat_data, 'result.transfer.transfer_reason') != 'null'
                  AND s.user_number != '+19259898099'
                GROUP BY transfer_reason
            ),
            org_totals AS (
                SELECT SUM(count) AS total_call_attempts
                FROM transfer_stats
            ),
            carrier_asked_stats AS (
                SELECT
                    ts.count AS carrier_asked_count,
                    ot.total_call_attempts,
                    ROUND((ts.count * 100.0) / ot.total_call_attempts, 2) AS carrier_asked_percentage
                FROM transfer_stats ts
                CROSS JOIN org_totals ot
                WHERE ts.transfer_reason = 'CARRIER_ASKED_FOR_TRANSFER'
            )
            SELECT
                carrier_asked_count,
                total_call_attempts,
                carrier_asked_percentage
            FROM carrier_asked_stats
            LIMIT 1
        """

def calls_ending_in_each_call_stage_stats_query(date_filter: str, org_id: str, PEPSI_BROKER_NODE_ID: str) -> str:
    # percentage of calls ending in each call stage
    return f"""
        WITH recent_runs AS (
            SELECT id AS run_id
            FROM public_runs
            WHERE {date_filter}
        ),
        sessions AS (
            SELECT run_id, user_number FROM public_sessions
            WHERE {date_filter}
            AND org_id = '{org_id}'
        ),
        call_stage_stats AS (
            SELECT
                JSONExtractString(no.flat_data, 'result.call.call_stage') AS call_stage,
                COUNT(*) AS count
            FROM public_node_outputs no
            INNER JOIN recent_runs rr ON no.run_id = rr.run_id
            INNER JOIN public_nodes n ON no.node_id = n.id
            INNER JOIN sessions s ON no.run_id = s.run_id
            WHERE n.org_id = '{org_id}'
                AND no.node_persistent_id = '{PEPSI_BROKER_NODE_ID}'
                AND JSONHas(no.flat_data, 'result.call.call_stage') = 1
                AND JSONExtractString(no.flat_data, 'result.call.call_stage') != ''
                AND JSONExtractString(no.flat_data, 'result.call.call_stage') != 'null'
                AND s.user_number != '+19259898099'
            GROUP BY call_stage
        ),
        total_calls AS (
            SELECT SUM(count) AS total FROM call_stage_stats
        )
        SELECT
            css.call_stage,
            css.count,
            ROUND((css.count * 100.0) / tc.total, 2) AS percentage
        FROM call_stage_stats css
        CROSS JOIN total_calls tc
        ORDER BY css.count DESC
    """

def load_not_found_stats_query(date_filter: str, org_id: str, PEPSI_BROKER_NODE_ID: str) -> str:
# percentage of calls where the load not found error is thrown
    return f"""
        WITH recent_runs AS (
        SELECT id AS run_id
        FROM public_runs
        WHERE {date_filter}
    ),
    sessions AS (
        SELECT run_id, user_number FROM public_sessions
        WHERE {date_filter}
        AND org_id = '{org_id}'
    ),
    extracted AS (
        SELECT
            JSONExtractString(no.flat_data, 'result.load.load_status') AS load_status
        FROM public_node_outputs AS no
        INNER JOIN recent_runs rr ON no.run_id = rr.run_id
        INNER JOIN public_nodes n ON no.node_id = n.id
        INNER JOIN sessions s ON no.run_id = s.run_id
        WHERE n.org_id = '{org_id}'
        AND no.node_persistent_id = '{PEPSI_BROKER_NODE_ID}'
        AND s.user_number != '+19259898099'
    ),
    load_status_stats AS (
        SELECT
            load_status,
            count() AS cnt
        FROM extracted
        WHERE isNotNull(load_status)
        AND load_status != ''
        AND load_status != 'null'
        GROUP BY load_status
    ),
    load_not_found_count AS (
        SELECT sum(cnt) AS load_not_found_count
        FROM load_status_stats
        WHERE load_status = 'NOT_FOUND'
    ),
    total_calls AS (
        SELECT sum(cnt) AS total_calls
        FROM load_status_stats
    )
    SELECT
        lnf.load_not_found_count,
        tc.total_calls,
        ifNull(round((lnf.load_not_found_count * 100.0) / nullIf(tc.total_calls, 0), 2), 0) AS load_not_found_percentage
    FROM load_not_found_count lnf
    CROSS JOIN total_calls tc

        """


def load_status_stats_query(date_filter: str, org_id: str, PEPSI_BROKER_NODE_ID: str) -> str:
    return f"""
        WITH recent_runs AS (
        SELECT id AS run_id
        FROM public_runs
        WHERE {date_filter}
    ),
    sessions AS (
        SELECT run_id, user_number FROM public_sessions
        WHERE {date_filter}
        AND org_id = '{org_id}'
    ),
    extracted AS (
        SELECT
            JSONExtractString(no.flat_data, 'result.load.load_status') AS load_status
        FROM public_node_outputs AS no
        INNER JOIN recent_runs rr ON no.run_id = rr.run_id
        INNER JOIN public_nodes n ON no.node_id = n.id
        INNER JOIN sessions s ON no.run_id = s.run_id
        WHERE n.org_id = '{org_id}'
        AND no.node_persistent_id = '{PEPSI_BROKER_NODE_ID}'
        AND s.user_number != '+19259898099'
    ),
    load_status_stats AS (
        SELECT
            load_status,
            count() AS cnt
        FROM extracted
        WHERE isNotNull(load_status)
        AND load_status != ''
        AND load_status != 'null'
        GROUP BY load_status
    ),
    total_calls AS (
        SELECT sum(cnt) AS total_calls
        FROM load_status_stats
    )
    SELECT
        lss.load_status,
        lss.cnt AS count,
        any(tc.total_calls) AS total_calls,
        ifNull(round((lss.cnt * 100.0) / nullIf(any(tc.total_calls), 0), 2), 0) AS load_status_percentage
    FROM load_status_stats lss
    CROSS JOIN total_calls tc
    GROUP BY lss.load_status, lss.cnt
    """

def successfully_transferred_for_booking_stats_query(date_filter: str, org_id: str, PEPSI_BROKER_NODE_ID: str) -> str:
    # percentage of calls where the transfer was successful for booking
    return f"""
        WITH recent_runs AS (
        SELECT id AS run_id
        FROM public_runs
        WHERE {date_filter}
    ),
    sessions AS (
        SELECT run_id, user_number FROM public_sessions
        WHERE {date_filter}
        AND org_id = '{org_id}'
    ),
    extracted AS (
        SELECT
            JSONExtractString(no.flat_data, 'result.transfer.transfer_attempt') AS transfer_attempt,
            JSONExtractString(no.flat_data, 'result.transfer.transfer_success') AS transfer_success,
            JSONExtractString(no.flat_data, 'result.pricing.agreed_upon_rate') AS agreed_upon_rate,
            JSONExtractString(no.flat_data, 'result.pricing.pricing_notes') AS pricing_notes
        FROM public_node_outputs AS no
        INNER JOIN recent_runs rr ON no.run_id = rr.run_id
        INNER JOIN public_nodes n ON no.node_id = n.id
        INNER JOIN sessions s ON no.run_id = s.run_id
        WHERE n.org_id = '{org_id}'
        AND no.node_persistent_id = '{PEPSI_BROKER_NODE_ID}'
        AND JSONHas(no.flat_data, 'result.transfer.transfer_attempt') = 1
        AND JSONHas(no.flat_data, 'result.transfer.transfer_success') = 1
        AND JSONHas(no.flat_data, 'result.pricing.agreed_upon_rate') = 1
        AND JSONHas(no.flat_data, 'result.pricing.pricing_notes') = 1
        AND s.user_number != '+19259898099'
    ),
    successfully_transferred_for_booking AS (
        SELECT
            transfer_attempt,
            transfer_success,
            agreed_upon_rate,
            pricing_notes,
            count() AS cnt
        FROM extracted
        WHERE isNotNull(transfer_attempt)
        AND transfer_attempt != ''
        AND transfer_attempt != 'null'
        AND transfer_attempt = 'YES'
        AND isNotNull(transfer_success)
        AND transfer_success != ''
        AND transfer_success != 'null'
        AND transfer_success = 'YES'
        AND isNotNull(agreed_upon_rate)
        AND agreed_upon_rate != ''
        AND agreed_upon_rate != 'null'
        AND isNotNull(pricing_notes)
        AND pricing_notes != ''
        AND pricing_notes != 'null'
        AND (pricing_notes = 'AGREEMENT_REACHED_WITH_NEGOTIATION' OR pricing_notes = 'AGREEMENT_REACHED_WITHOUT_NEGOTIATION')
        GROUP BY transfer_attempt, transfer_success, agreed_upon_rate, pricing_notes
    ),
    successfully_transferred_for_booking_count AS (
        SELECT SUM(cnt) AS successfully_transferred_for_booking_count
        FROM successfully_transferred_for_booking
    ),
    total_calls AS (
        SELECT COUNT(*) AS total_calls FROM extracted
    )
    SELECT
        stfb.successfully_transferred_for_booking_count,
        tc.total_calls,
        ifNull(round((stfb.successfully_transferred_for_booking_count * 100.0) / nullIf(tc.total_calls, 0), 2), 0) AS successfully_transferred_for_booking_percentage
    FROM successfully_transferred_for_booking_count stfb, total_calls tc
    """

def call_classication_stats_query(date_filter: str, org_id: str, PEPSI_BROKER_NODE_ID: str) -> str:
    # percentage of calls ending in each call stage
    return f"""
        WITH recent_runs AS (
            SELECT id AS run_id
            FROM public_runs
            WHERE {date_filter}
        ),
        sessions AS (
            SELECT run_id, user_number FROM public_sessions
            WHERE {date_filter}
            AND org_id = '{org_id}'
        ),
        call_classification_stats AS (
            SELECT
                JSONExtractString(no.flat_data, 'result.call.call_classification') AS call_classification,
                COUNT(*) AS count
            FROM public_node_outputs no
            INNER JOIN recent_runs rr ON no.run_id = rr.run_id
            INNER JOIN public_nodes n ON no.node_id = n.id
            INNER JOIN sessions s ON no.run_id = s.run_id
            WHERE n.org_id = '{org_id}'
            AND no.node_persistent_id = '{PEPSI_BROKER_NODE_ID}'
            AND JSONHas(no.flat_data, 'result.call.call_classification') = 1
            AND JSONExtractString(no.flat_data, 'result.call.call_classification') != ''
            AND JSONExtractString(no.flat_data, 'result.call.call_classification') != 'null'
            AND s.user_number != '+19259898099'
            GROUP BY call_classification
        ),
        total_calls AS (
            SELECT SUM(count) AS total FROM call_classification_stats
        )
        SELECT
            ccs.call_classification,
            ccs.count,
            ROUND((ccs.count * 100.0) / tc.total, 2) AS percentage
        FROM call_classification_stats ccs
        CROSS JOIN total_calls tc
        ORDER BY ccs.count DESC
    """


def carrier_qualification_stats_query(date_filter: str, org_id: str, PEPSI_BROKER_NODE_ID: str) -> str:
    # percentage of calls where the carrier was qualified
    return f"""
        WITH recent_runs AS (
            SELECT id AS run_id
            FROM public_runs
            WHERE {date_filter}
        ),
        sessions AS (
            SELECT run_id, user_number FROM public_sessions
            WHERE {date_filter}
            AND org_id = '{org_id}'
        ),
        carrier_qualification_stats AS (
            SELECT
                JSONExtractString(no.flat_data, 'result.carrier.carrier_qualification') AS carrier_qualification,
                COUNT(*) AS count
            FROM public_node_outputs no
            INNER JOIN recent_runs rr ON no.run_id = rr.run_id
            INNER JOIN public_nodes n ON no.node_id = n.id
            INNER JOIN sessions s ON no.run_id = s.run_id
            WHERE n.org_id = '{org_id}'
            AND no.node_persistent_id = '{PEPSI_BROKER_NODE_ID}'
            AND JSONHas(no.flat_data, 'result.carrier.carrier_qualification') = 1
            AND JSONExtractString(no.flat_data, 'result.carrier.carrier_qualification') != ''
            AND JSONExtractString(no.flat_data, 'result.carrier.carrier_qualification') != 'null'
            AND s.user_number != '+19259898099'
            GROUP BY carrier_qualification
        ),
        total_calls AS (
            SELECT SUM(count) AS total FROM carrier_qualification_stats
        )
        SELECT
            cqs.carrier_qualification,
            cqs.count,
            ROUND((cqs.count * 100.0) / tc.total, 2) AS percentage
        FROM carrier_qualification_stats cqs
        CROSS JOIN total_calls tc
        ORDER BY cqs.count DESC
    """

def pricing_stats_query(date_filter: str, org_id: str, PEPSI_BROKER_NODE_ID: str) -> str:
    # pricing stats
    return f"""
        WITH recent_runs AS (
            SELECT id AS run_id
            FROM public_runs
            WHERE {date_filter}
        ),
        sessions AS (
        select run_id, user_number from public_sessions
        WHERE {date_filter}
        AND org_id = '{org_id}'
        ),
        pricing_stats AS (
            SELECT
                JSONExtractString(no.flat_data, 'result.pricing.pricing_notes') AS pricing_notes,
                COUNT(*) AS count
            FROM public_node_outputs no
            INNER JOIN recent_runs rr ON no.run_id = rr.run_id
            INNER JOIN public_nodes n ON no.node_id = n.id
            INNER JOIN sessions s ON no.run_id = s.run_id
            WHERE n.org_id = '{org_id}'
            AND no.node_persistent_id = '{PEPSI_BROKER_NODE_ID}'
            AND JSONHas(no.flat_data, 'result.pricing.pricing_notes') = 1
            AND JSONExtractString(no.flat_data, 'result.pricing.pricing_notes') != ''
            AND JSONExtractString(no.flat_data, 'result.pricing.pricing_notes') != 'null'
            AND s.user_number != '+19259898099'
            GROUP BY pricing_notes
        ),
        total_calls AS (
            SELECT SUM(count) AS total FROM pricing_stats
        )
        SELECT
            ps.pricing_notes,
            ps.count,
            ROUND((ps.count * 100.0) / tc.total, 2) AS percentage
        FROM pricing_stats ps
        CROSS JOIN total_calls tc
        ORDER BY ps.count DESC
    """