human_v2 = """Query: {query}
- Possible rewrites for this query: {query_rewrites}
- Concepts in this query: {query_entities}

Product:
- Title: {title}
- Shop name: {shop}
- Product image caption: {image}
- Unigrams & bigrams from description: {description}
- Category: {category}
- Attributes: {attribute}
- Tags: {tag}
- Custom options: {variation}
- Reviews: {review}"""


human_v3 = """Query: {query}
- Concepts in this query: {query_entities}
- Possible rewrites for this query: {query_rewrites}

Product:
- Title: {title}
- Shop name: {shop}
- Image caption: {image}
- Description key words and terms: {description}
- Attributes: {attribute}
- Custom options: {variation}"""