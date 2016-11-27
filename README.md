# Extract Valid wikilinks and Generate Adjacency Graph
I have used the wikilinks data set downloadable here:
https://s3.amazonaws.com/uf‐ids‐data/enwiki‐latest‐pages‐articles.xml

The project can be divided into two parts:
    A. Writing a MapReduce job that extracts wikilinks and also remove all the red links.  
    B. Writing another MapReduce job to generate the Adjacency graph, i.e a graph with the format:
            page_title1     link1     link2 … 
            page_titile2    link2     link3 …
            With each line representing a page, and each item separated by tabs..
