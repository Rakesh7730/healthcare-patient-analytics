# Gold dbt Models

These are the GitHub-ready Gold-layer dbt models for the Healthcare Patient Analytics Pipeline.

## Models

- `dim_date.sql`
- `dim_patient.sql`
- `dim_hospital.sql`
- `dim_observation.sql`
- `fact_health_metrics.sql`

## Corrections applied

1. Renamed `dim_patients.sql` to `dim_patient.sql` to match `ref('dim_patient')`.
2. Renamed `dim_observations.sql` to `dim_observation.sql` to match `ref('dim_observation')`.
3. Added `doctor_name` and `technician_name` to the observation hash inputs to reduce key collisions.
4. Added `doctor_name` to the diagnosis-to-observation join so multiple doctors for the same diagnosis/date do not multiply fact rows.
5. Corrected the patient-risk weights from 105% to 100%:
   - risk probability: 40%
   - severity: 25%
   - comorbidity: 15%
   - lifestyle: 10%
   - smoking/alcohol: 5%
   - quality flag: 5%

## Important validation

The risk-score formula assumes `risk_probability` is already normalized to 0-1, while
`comorbidity_score` and `lifestyle_risk` are 0-100. Verify those scales against the
Silver models before production use.

Also run:

```bash
dbt parse
dbt compile --select dim_date dim_patient dim_hospital dim_observation fact_health_metrics
dbt build --select dim_date dim_patient dim_hospital dim_observation fact_health_metrics
```

Do not commit credentials, tokens, `.env` files, or `profiles.yml` containing secrets.
