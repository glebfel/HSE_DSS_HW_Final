from etl.db import get_connection


def main():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 'stage_rows' AS metric, count(*) AS value FROM staging.sample_superstore
                UNION ALL
                SELECT 'hub_ship_mode', count(*) FROM datavault.hub_ship_mode
                UNION ALL
                SELECT 'hub_segment', count(*) FROM datavault.hub_segment
                UNION ALL
                SELECT 'hub_geography', count(*) FROM datavault.hub_geography
                UNION ALL
                SELECT 'hub_category', count(*) FROM datavault.hub_category
                UNION ALL
                SELECT 'hub_sub_category', count(*) FROM datavault.hub_sub_category
                UNION ALL
                SELECT 'link_sale', count(*) FROM datavault.link_sale
                UNION ALL
                SELECT 'sat_sale_metrics', count(*) FROM datavault.sat_sale_metrics
                """
            )
            rows = cur.fetchall()
            print({"counts": [{"metric": r[0], "value": int(r[1])} for r in rows]})

            cur.execute(
                """
                SELECT g.bk_region AS region, c.bk_category AS category,
                       sum(sm.sales) AS total_sales, sum(sm.profit) AS total_profit
                FROM datavault.sat_sale_metrics sm
                JOIN datavault.link_sale l ON l.sale_hk = sm.sale_hk
                JOIN datavault.hub_geography g ON g.geography_hk = l.geography_hk
                JOIN datavault.hub_category c ON c.category_hk = l.category_hk
                GROUP BY g.bk_region, c.bk_category
                ORDER BY total_sales DESC
                LIMIT 20
                """
            )
            rows = cur.fetchall()
            results = [
                {
                    "region": r[0],
                    "category": r[1],
                    "total_sales": float(r[2]) if r[2] is not None else None,
                    "total_profit": float(r[3]) if r[3] is not None else None,
                }
                for r in rows
            ]
            print({"top_sales_by_region_category": results})


if __name__ == "__main__":
    main()