--empty flag : build and create every tables within your DWH, all structure are built without any lines. It validates the entire pipeline without data.

--sample flag : build and create every tables within your DWH based on time provided ->

--sample="3 days" / --sample="6 hours" / --sample="{'start': '2024-07-01', 'end': '2024-07-08 18:00:00'}"

Also for static tables or seeds that have static data you might want to add the render() to avoid your pipeline to build your model with only the stores created or updated within the last 3 days...
Adding render() will tell dbt to render the whole table/seed.

select * from {{ ref('stg_customers').render() }}