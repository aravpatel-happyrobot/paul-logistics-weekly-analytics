
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
            JSONExtractString(no.flat_data, 'result.pricing.agreed_upon_rate') AS agreed_upon_rate,
            JSONExtractString(no.flat_data, 'result.pricing.pricing_notes') AS pricing_notes
        FROM public_node_outputs AS no
        INNER JOIN recent_runs rr ON no.run_id = rr.run_id
        INNER JOIN public_nodes n ON no.node_id = n.id
        INNER JOIN sessions s ON no.run_id = s.run_id
        WHERE n.org_id = '{org_id}'
        AND no.node_persistent_id = '{PEPSI_BROKER_NODE_ID}'
        AND JSONHas(no.flat_data, 'result.transfer.transfer_attempt') = 1
        AND JSONHas(no.flat_data, 'result.pricing.agreed_upon_rate') = 1
        AND JSONHas(no.flat_data, 'result.pricing.pricing_notes') = 1
        AND s.user_number != '+19259898099'
    ),
    successfully_transferred_for_booking AS (
        SELECT
            transfer_attempt,
            agreed_upon_rate,
            pricing_notes,
            count() AS cnt
        FROM extracted
        WHERE isNotNull(transfer_attempt)
        AND transfer_attempt != ''
        AND transfer_attempt != 'null'
        AND transfer_attempt = 'YES'
        AND isNotNull(agreed_upon_rate)
        AND agreed_upon_rate != ''
        AND agreed_upon_rate != 'null'
        AND isNotNull(pricing_notes)
        AND pricing_notes != ''
        AND pricing_notes != 'null'
        AND (pricing_notes = 'AGREEMENT_REACHED_WITH_NEGOTIATION' OR pricing_notes = 'AGREEMENT_REACHED_WITHOUT_NEGOTIATION')
        GROUP BY transfer_attempt, agreed_upon_rate, pricing_notes
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

def call_classifcation_stats_query(date_filter: str, org_id: str, PEPSI_BROKER_NODE_ID: str) -> str:
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

    # return f"""
    #     WITH recent_runs AS (
    #         SELECT id AS run_id
    #         FROM public_runs
    #         WHERE {date_filter}
    #     ),
    #     sessions AS (
    #         SELECT run_id, user_number FROM public_sessions
    #         WHERE {date_filter}
    #         AND org_id = '{org_id}'
    #     ),
    #     pricing_stats AS (
    #         SELECT
    #             JSONExtractString(no.flat_data, 'result.pricing.pricing_notes') AS pricing_notes,
    #             no.run_id AS run_id,
    #             COUNT(*) AS count
    #         FROM public_node_outputs no
    #         INNER JOIN recent_runs rr ON no.run_id = rr.run_id
    #         INNER JOIN public_nodes n ON no.node_id = n.id
    #         INNER JOIN sessions s ON no.run_id = s.run_id
    #         WHERE n.org_id = '{org_id}'
    #         AND no.node_persistent_id = '{PEPSI_BROKER_NODE_ID}'
    #         AND JSONHas(no.flat_data, 'result.pricing.pricing_notes') = 1
    #         AND JSONExtractString(no.flat_data, 'result.pricing.pricing_notes') != ''
    #         AND JSONExtractString(no.flat_data, 'result.pricing.pricing_notes') != 'null'
    #         AND s.user_number != '+19259898099'
    #         GROUP BY pricing_notes, run_id
    #     ),
    #     call_classification_stats AS (
    #         SELECT
    #             JSONExtractString(no.flat_data, 'result.call.call_classification') AS call_classification,
    #             COUNT(*) AS count
    #         FROM public_node_outputs no
    #         INNER JOIN recent_runs rr ON no.run_id = rr.run_id
    #         INNER JOIN public_nodes n ON no.node_id = n.id
    #         INNER JOIN sessions s ON no.run_id = s.run_id
    #         INNER JOIN pricing_stats ps ON no.run_id = ps.run_id
    #         WHERE n.org_id = '{org_id}'
    #         AND no.node_persistent_id = '{PEPSI_BROKER_NODE_ID}'
    #         AND JSONHas(no.flat_data, 'result.call.call_classification') = 1
    #         AND JSONExtractString(no.flat_data, 'result.call.call_classification') != ''
    #         AND JSONExtractString(no.flat_data, 'result.call.call_classification') != 'null'
    #         AND s.user_number != '+19259898099'
    #         -- Only count success if pricing.pricing_notes match
    #         AND (
    #             JSONExtractString(no.flat_data, 'result.call.call_classification') != 'success'
    #             OR (
    #                 JSONExtractString(no.flat_data, 'result.call.call_classification') = 'success'
    #                 AND JSONExtractString(no.flat_data, 'result.pricing.pricing_notes') IN (
    #                     'AGREEMENT_REACHED_WITH_NEOGTIATION',
    #                     'AGREEMENT_REACHED_WITHOUT_NEOGTIATION'
    #                 )
    #             )
    #         )
    #         GROUP BY call_classification
    #     ),
    #     total_calls AS (
    #         SELECT SUM(count) AS total FROM call_classification_stats
    #     )
    #     SELECT
    #         ccs.call_classification,
    #         ccs.count,
    #         ROUND((ccs.count * 100.0) / tc.total, 2) AS percentage
    #     FROM call_classification_stats ccs
    #     CROSS JOIN total_calls tc
    #     ORDER BY ccs.count DESC
    # """



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


def carrier_end_state_query(date_filter: str, org_id: str, PEPSI_BROKER_NODE_ID: str) -> str:
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
        carrier_end_state_stats AS (
            SELECT
                JSONExtractString(no.flat_data, 'result.carrier.carrier_end_state') AS carrier_end_state,
                COUNT(*) AS count
            FROM public_node_outputs no
            INNER JOIN recent_runs rr ON no.run_id = rr.run_id
            INNER JOIN public_nodes n ON no.node_id = n.id
            INNER JOIN sessions s ON no.run_id = s.run_id
            WHERE n.org_id = '{org_id}'
            AND no.node_persistent_id = '{PEPSI_BROKER_NODE_ID}'
            AND JSONHas(no.flat_data, 'result.carrier.carrier_end_state') = 1
            AND JSONExtractString(no.flat_data, 'result.carrier.carrier_end_state') != ''
            AND JSONExtractString(no.flat_data, 'result.carrier.carrier_end_state') != 'null'
            AND s.user_number != '+19259898099'
            GROUP BY carrier_end_state
        ),
        total_calls AS (
            SELECT SUM(count) AS total FROM carrier_end_state_stats
        )
        SELECT
            ces.carrier_end_state,
            ces.count,
            ROUND((ces.count * 100.0) / tc.total, 2) AS percentage
        FROM carrier_end_state_stats ces
        CROSS JOIN total_calls tc
        ORDER BY ces.count DESC
    """


def percent_non_convertible_calls_query(date_filter: str, org_id: str, PEPSI_BROKER_NODE_ID: str) -> str:
    # percentage of non convertible calls
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
            AND isNotNull(user_number)
            AND user_number != ''
            AND user_number != '+19259898099'
        ),
        non_convertible_calls_stats AS (
            SELECT *
            FROM public_node_outputs no
            INNER JOIN recent_runs rr ON no.run_id = rr.run_id
            INNER JOIN public_nodes n ON no.node_id = n.id
            INNER JOIN sessions s ON no.run_id = s.run_id
            WHERE n.org_id = '{org_id}'
            AND no.node_persistent_id = '{PEPSI_BROKER_NODE_ID}'
            AND (JSONExtractString(no.flat_data, 'result.load.load_status') = 'COVERED'
            OR JSONExtractString(no.flat_data, 'result.load.load_status') = 'PAST_DUE'
            OR JSONExtractString(no.flat_data, 'result.carrier.carrier_end_state') = 'CARRIER_OFFER_TOO_HIGH'
            OR JSONExtractString(no.flat_data, 'result.carrier.carrier_end_state') = 'CARRIER_UNABLE_TO_MEET_PICKUP_DELIVERY_APPT'
            OR JSONExtractString(no.flat_data, 'result.carrier.carrier_end_state') = 'CARRIER_UNABLE_TO_MEET_EQUIPMENT_REQ'
            OR JSONExtractString(no.flat_data, 'result.carrier.carrier_end_state') = 'CARRIER_DID_NOT_WANT_LOAD'
            OR JSONExtractString(no.flat_data, 'result.call.call_classification') = 'rate_too_high'
            )
            AND s.user_number != '+19259898099'
        ),
        non_convertible_calls_count AS (
            SELECT SUM(1) AS non_convertible_calls_count FROM non_convertible_calls_stats
        ),
        total_calls AS (
            SELECT SUM(1) AS total_calls FROM sessions
        )
        SELECT non_convertible_calls_count, total_calls, ROUND((non_convertible_calls_count * 100.0) / total_calls, 2) AS non_convertible_calls_percentage
        FROM non_convertible_calls_count, total_calls
        """

def number_of_unique_loads_query(date_filter: str, org_id: str, PEPSI_FBR_NODE_ID: str) -> str:
    # number of unique loads
    return f"""
        WITH recent_runs AS (
            SELECT id AS run_id
            FROM public_runs
            WHERE {date_filter}
        ),
        sessions AS (
            SELECT DISTINCT s.run_id, s.user_number 
            FROM public_sessions s
            INNER JOIN recent_runs rr ON s.run_id = rr.run_id
            WHERE s.org_id = '{org_id}'
            AND isNotNull(s.user_number)
            AND s.user_number != ''
            AND s.user_number != '+19259898099'
        ),
        number_of_unique_loads_stats AS (
            SELECT uniqExact(JSONExtractString(no.flat_data, 'load.custom_load_id')) AS number_of_unique_loads
            FROM public_node_outputs no
            INNER JOIN recent_runs rr ON no.run_id = rr.run_id
            INNER JOIN public_nodes n ON no.node_id = n.id
            INNER JOIN sessions s ON no.run_id = s.run_id
            WHERE n.org_id = '{org_id}'
            AND no.node_persistent_id = '{PEPSI_FBR_NODE_ID}'
            AND JSONHas(no.flat_data, 'load.custom_load_id') = 1
            AND JSONExtractString(no.flat_data, 'load.custom_load_id') != ''
            AND JSONExtractString(no.flat_data, 'load.custom_load_id') != 'null'
            AND s.user_number != '+19259898099'
        ),
        total_calls AS (
            SELECT SUM(1) AS total_calls FROM sessions
        )
        SELECT 
            number_of_unique_loads, 
            total_calls, 
            ifNull(round(total_calls / nullIf(number_of_unique_loads, 0), 2), 0) AS calls_per_unique_load
        FROM number_of_unique_loads_stats, total_calls
        """

def list_of_unique_loads_query(date_filter: str, org_id: str, PEPSI_FBR_NODE_ID: str) -> str:
    # list of unique loads
    return f"""
        WITH recent_runs AS (
            SELECT id AS run_id
            FROM public_runs
            WHERE {date_filter}
        ),
        sessions AS (
            SELECT DISTINCT s.run_id, s.user_number 
            FROM public_sessions s
            INNER JOIN recent_runs rr ON s.run_id = rr.run_id
            WHERE s.org_id = '{org_id}'
            AND isNotNull(s.user_number)
            AND s.user_number != ''
            AND s.user_number != '+19259898099'
        ),
        list_of_unique_loads_stats AS (
            SELECT DISTINCT JSONExtractString(no.flat_data, 'load.custom_load_id') AS custom_load_id
            FROM public_node_outputs no
            INNER JOIN recent_runs rr ON no.run_id = rr.run_id
            INNER JOIN public_nodes n ON no.node_id = n.id
            INNER JOIN sessions s ON no.run_id = s.run_id
            WHERE n.org_id = '{org_id}'
            AND no.node_persistent_id = '{PEPSI_FBR_NODE_ID}'
            AND JSONHas(no.flat_data, 'load.custom_load_id') = 1
            AND JSONExtractString(no.flat_data, 'load.custom_load_id') != ''
            AND JSONExtractString(no.flat_data, 'load.custom_load_id') != 'null'
            AND s.user_number != '+19259898099'
        )
        SELECT custom_load_id FROM list_of_unique_loads_stats
        """

def number_of_unique_loads_query_broker_node(date_filter: str, org_id: str, PEPSI_BROKER_NODE_ID: str) -> str:
    # number of unique loads
    return f"""
        WITH recent_runs AS (
            SELECT id AS run_id
            FROM public_runs
            WHERE {date_filter}
        ),
        sessions AS (
            SELECT DISTINCT s.run_id, s.user_number 
            FROM public_sessions s
            INNER JOIN recent_runs rr ON s.run_id = rr.run_id
            WHERE s.org_id = '{org_id}'
            AND isNotNull(s.user_number)
            AND s.user_number != ''
            AND s.user_number != '+19259898099'
        ),
        number_of_unique_loads_stats AS (
            SELECT uniqExact(JSONExtractString(no.flat_data, 'result.load.reference_number')) AS number_of_unique_loads
            FROM public_node_outputs no
            INNER JOIN recent_runs rr ON no.run_id = rr.run_id
            INNER JOIN public_nodes n ON no.node_id = n.id
            INNER JOIN sessions s ON no.run_id = s.run_id
            WHERE n.org_id = '{org_id}'
            AND no.node_persistent_id = '{PEPSI_BROKER_NODE_ID}'
            AND JSONHas(no.flat_data, 'result.load.reference_number') = 1
            AND JSONExtractString(no.flat_data, 'result.load.reference_number') != ''
            AND JSONExtractString(no.flat_data, 'result.load.reference_number') != 'null'
            AND s.user_number != '+19259898099'
        ),
        total_calls AS (
            SELECT SUM(1) AS total_calls FROM sessions
        )
        SELECT 
            number_of_unique_loads, 
            total_calls, 
            ifNull(round(total_calls / nullIf(number_of_unique_loads, 0), 2), 0) AS calls_per_unique_load
        FROM number_of_unique_loads_stats, total_calls
        """

def list_of_unique_loads_query_broker_node(date_filter: str, org_id: str, PEPSI_BROKER_NODE_ID: str) -> str:
    # list of unique loads
    return f"""
        WITH recent_runs AS (
            SELECT id AS run_id
            FROM public_runs
            WHERE {date_filter}
        ),
        sessions AS (
            SELECT DISTINCT s.run_id, s.user_number 
            FROM public_sessions s
            INNER JOIN recent_runs rr ON s.run_id = rr.run_id
            WHERE s.org_id = '{org_id}'
            AND isNotNull(s.user_number)
            AND s.user_number != ''
            AND s.user_number != '+19259898099'
        ),
        list_of_unique_loads_stats AS (
            SELECT DISTINCT JSONExtractString(no.flat_data, 'result.load.reference_number') AS custom_load_id
            FROM public_node_outputs no
            INNER JOIN recent_runs rr ON no.run_id = rr.run_id
            INNER JOIN public_nodes n ON no.node_id = n.id
            INNER JOIN sessions s ON no.run_id = s.run_id
            WHERE n.org_id = '{org_id}'
            AND no.node_persistent_id = '{PEPSI_BROKER_NODE_ID}'
            AND JSONHas(no.flat_data, 'result.load.reference_number') = 1
            AND JSONExtractString(no.flat_data, 'result.load.reference_number') != ''
            AND JSONExtractString(no.flat_data, 'result.load.reference_number') != 'null'
            AND s.user_number != '+19259898099'
        )
        SELECT custom_load_id FROM list_of_unique_loads_stats
        """

def calls_without_carrier_asked_for_transfer_query(
    date_filter: str, org_id: str, PEPSI_BROKER_NODE_ID: str
) -> str:
    # calls without carrier asked for transfer
    return f"""
        WITH recent_runs AS (
            SELECT id AS run_id
            FROM public_runs
            WHERE {date_filter}
        ),
        sessions AS (
            SELECT run_id, user_number, duration
            FROM public_sessions
            WHERE {date_filter}
              AND org_id = '{org_id}'
              AND user_number != '+19259898099'
        ),
        -- Base CTE: runs that have transfer_reason set and not carrier_asked_for_transfer
        eligible_runs AS (
            SELECT DISTINCT no.run_id as run_id
            FROM public_node_outputs no
            INNER JOIN recent_runs rr ON no.run_id = rr.run_id
            INNER JOIN public_nodes n ON no.node_id = n.id
            INNER JOIN sessions s ON no.run_id = s.run_id
            WHERE n.org_id = '{org_id}'
              AND no.node_persistent_id = '{PEPSI_BROKER_NODE_ID}'
              AND JSONHas(no.flat_data, 'result.transfer.transfer_reason') = 1
              AND JSONExtractString(no.flat_data, 'result.transfer.transfer_reason') != ''
              AND JSONExtractString(no.flat_data, 'result.transfer.transfer_reason') != 'null'
              AND upper(JSONExtractString(no.flat_data, 'result.transfer.transfer_reason')) != 'CARRIER_ASKED_FOR_TRANSFER'
              AND s.user_number != '+19259898099'
        ),
        -- Per-run stats for calls where transfer_reason is set and not carrier_asked_for_transfer
        total_calls_without_carrier_asked_for_transfer_stats AS (
            SELECT
                s.run_id as run_id,
                sum(s.duration) AS duration,
                count(*) AS total_calls
            FROM sessions s
            INNER JOIN eligible_runs er ON s.run_id = er.run_id
            GROUP BY s.run_id
        ),
        -- Overall totals across all those runs
        total_calls_without_carrier_asked_for_transfer_overall AS (
            SELECT
                sum(total_calls)   AS total_calls,
                sum(duration)      AS total_duration
            FROM total_calls_without_carrier_asked_for_transfer_stats
        ),
        non_convertible_calls_stats AS (
            SELECT
                count(DISTINCT no.run_id) AS non_convertible_calls_count,
                sum(DISTINCT s.duration) AS total_duration
            FROM public_node_outputs no
            INNER JOIN recent_runs rr ON no.run_id = rr.run_id
            INNER JOIN public_nodes n ON no.node_id = n.id
            INNER JOIN sessions s ON no.run_id = s.run_id
            INNER JOIN eligible_runs er ON no.run_id = er.run_id
            WHERE n.org_id = '{org_id}'
              AND no.node_persistent_id = '{PEPSI_BROKER_NODE_ID}'
              AND JSONHas(no.flat_data, 'result.call.call_classification') = 1
              AND JSONExtractString(no.flat_data, 'result.call.call_classification') IN (
                  'alternate_equipment',
                  'caller_hung_up_no_explanation',
                  'load_not_ready',
                  'load_past_due',
                  'covered',
                  'carrier_not_qualified',
                  'alternate_date_or_time',
                  'user_declined_load',
                  'checking_with_driver',
                  'carrier_cannot_see_reference_number',
                  'caller_put_on_hold_assistant_hung_up'
              )
        ),
        rate_too_high_calls_stats AS (
            SELECT
                count(DISTINCT no.run_id) AS non_convertible_calls_count,
                sum(DISTINCT s.duration) AS total_duration
            FROM public_node_outputs no
            INNER JOIN recent_runs rr ON no.run_id = rr.run_id
            INNER JOIN public_nodes n ON no.node_id = n.id
            INNER JOIN sessions s ON no.run_id = s.run_id
            INNER JOIN eligible_runs er ON no.run_id = er.run_id
            WHERE n.org_id = '{org_id}'
              AND no.node_persistent_id = '{PEPSI_BROKER_NODE_ID}'
              AND JSONHas(no.flat_data, 'result.call.call_classification') = 1
              AND JSONExtractString(no.flat_data, 'result.call.call_classification') = 'rate_too_high'
        ),
        success_calls_stats AS (
            SELECT
                count(DISTINCT no.run_id) AS non_convertible_calls_count,
                sum(DISTINCT s.duration) AS total_duration
            FROM public_node_outputs no
            INNER JOIN recent_runs rr ON no.run_id = rr.run_id
            INNER JOIN public_nodes n ON no.node_id = n.id
            INNER JOIN sessions s ON no.run_id = s.run_id
            INNER JOIN eligible_runs er ON no.run_id = er.run_id
            WHERE n.org_id = '{org_id}'
              AND no.node_persistent_id = '{PEPSI_BROKER_NODE_ID}'
              AND JSONHas(no.flat_data, 'result.call.call_classification') = 1
              AND JSONExtractString(no.flat_data, 'result.call.call_classification') IN ('success', 'after_hours')
        ),
        other_calls_stats AS (
            SELECT
                count(DISTINCT no.run_id) AS non_convertible_calls_count,
                sum(DISTINCT s.duration) AS total_duration
            FROM public_node_outputs no
            INNER JOIN recent_runs rr ON no.run_id = rr.run_id
            INNER JOIN public_nodes n ON no.node_id = n.id
            INNER JOIN sessions s ON no.run_id = s.run_id
            INNER JOIN eligible_runs er ON no.run_id = er.run_id
            WHERE n.org_id = '{org_id}'
              AND no.node_persistent_id = '{PEPSI_BROKER_NODE_ID}'
              AND JSONHas(no.flat_data, 'result.call.call_classification') = 1
              AND JSONExtractString(no.flat_data, 'result.call.call_classification') = 'other'
        )
        SELECT
            non_convertible_calls_stats.non_convertible_calls_count AS non_convertible_calls_count,
            non_convertible_calls_stats.total_duration            AS non_convertible_calls_duration,
            rate_too_high_calls_stats.non_convertible_calls_count AS rate_too_high_calls_count,
            rate_too_high_calls_stats.total_duration              AS rate_too_high_calls_duration,
            success_calls_stats.non_convertible_calls_count       AS success_calls_count,
            success_calls_stats.total_duration                    AS success_calls_duration,
            other_calls_stats.non_convertible_calls_count         AS other_calls_count,
            other_calls_stats.total_duration                      AS other_calls_duration,
            total_overall.total_calls                             AS total_calls_no_carrier_asked_for_transfer,
            total_overall.total_duration                          AS total_duration_no_carrier_asked_for_transfer
        FROM
            non_convertible_calls_stats,
            rate_too_high_calls_stats,
            success_calls_stats,
            other_calls_stats,
            total_calls_without_carrier_asked_for_transfer_overall AS total_overall
        """

def total_calls_and_total_duration_query(date_filter: str, org_id: str, PEPSI_BROKER_NODE_ID: str) -> str:
    # total calls and total duration
    return f"""
        WITH recent_runs AS (
            SELECT id AS run_id
            FROM public_runs
            WHERE {date_filter}
        ),
        sessions AS (
            SELECT run_id, user_number, duration
            FROM public_sessions
            WHERE {date_filter}
              AND org_id = '{org_id}'
              AND user_number != '+19259898099'
        ),
        total_calls_and_total_duration_stats AS (
            SELECT
                sum(duration) AS total_duration,
                count(*) AS total_calls
            FROM sessions
            INNER JOIN public_node_outputs no on sessions.run_id = no.run_id
            WHERE no.node_persistent_id = '{PEPSI_BROKER_NODE_ID}'
        )
        SELECT total_duration, total_calls, (total_duration / total_calls) / 60 as avg_minutes_per_call
        FROM total_calls_and_total_duration_stats
        """

def duration_carrier_asked_for_transfer_query(date_filter: str, org_id: str, PEPSI_BROKER_NODE_ID: str) -> str:
    # duration of carrier asked for transfer
    return f"""
        WITH recent_runs AS (
            SELECT id AS run_id
            FROM public_runs
            WHERE {date_filter}
        ),
        sessions AS (
            SELECT run_id, user_number, duration
            FROM public_sessions
            WHERE {date_filter}
              AND org_id = '{org_id}'
              AND user_number != '+19259898099'
        ),
        carrier_asked_sessions AS (
            SELECT DISTINCT s.run_id as run_id, s.duration as duration
            FROM sessions s
            INNER JOIN recent_runs rr ON s.run_id = rr.run_id
            INNER JOIN public_node_outputs no ON s.run_id = no.run_id
            INNER JOIN public_nodes n ON no.node_id = n.id
            WHERE n.org_id = '{org_id}'
              AND no.node_persistent_id = '{PEPSI_BROKER_NODE_ID}'
              AND JSONHas(no.flat_data, 'result.transfer.transfer_reason') = 1
              AND JSONExtractString(no.flat_data, 'result.transfer.transfer_reason') != ''
              AND JSONExtractString(no.flat_data, 'result.transfer.transfer_reason') != 'null'
              AND upper(JSONExtractString(no.flat_data, 'result.transfer.transfer_reason')) = 'CARRIER_ASKED_FOR_TRANSFER'
        ),
        duration_carrier_asked_for_transfer_stats AS (
            SELECT
                ifNull(sum(duration), 0) AS duration_carrier_asked_for_transfer
            FROM carrier_asked_sessions
        )
        SELECT duration_carrier_asked_for_transfer
        FROM duration_carrier_asked_for_transfer_stats
    """