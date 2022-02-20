Opportunity Problem Challenge: Help a 25-year-old US Army veteran choose which college to attend
---

**Description**

Maria is a 25-year-old US Army veteran, newly returned to the civilian workforce.
She has recently completed a six-year commitment with the Army. During her time in the Army, 
she worked in supply management and logistics. She has decided to pursue a degree in Management
Systems and Information Technology.

Maria has asked you to use your skill with data to help her search for the best
school for her. She is willing to relocate anywhere in the continental United States, but she has a few criteria that her ideal schools must satisfy: 1) safety (low crime), 2) urban -- Maria wants to live the big city life, and 3) start-ups -- the school should be in a metropolitan area that ranks highly in entrepreneurialism (she plans to find an internship at a startup while she studies).

Maria would like you to help her narrow down her search to a list of schools that she can investigate more closely before making her decision.
Your Task:

    Produce a dataset of schools which satisfy all of Maria's criteria
    Rank them from best to worst according to the same criteria.

**Maria's schools must:**

1. be in an urban/metropolitan area.
2. be in a city that ranks 75th percentile or higher on Kauffman's start-up rankings.
3. be below 50th percentile in overall crime.
4. offer a 2-year or 4-year degree in Information Technology/Science.

**Extra Credit**

Maria doesn't like the cold. Find and integrate temperature data. Eliminate any schools located in cities/areas below the 25th percentile in average temperature.

**Tips:**

- Read the data dictionaries or codebooks to figure out what the variables mean and which ones you will need to use.

- Eliminate unneeded columns.

- Look for suitable columns to join the tables on.

- Perform any cleaning and standardization needed to facilitate the joins. 
- Engineer a summary variable for school crime so that we can compare schools by levels of crime overall.
- Eliminate from the data all the data points that fail to satisfy Maria's criteria.
- Engineer a method for ranking the schools in consideration of all of Maria's criteria taken together.

    
