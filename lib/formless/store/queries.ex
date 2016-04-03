defmodule Formless.Store.Queries do
  def shingle_to_cypher({shingle, side, subshingles}, bucket) do
    subshingles
    |> Enum.map(&shingle_subshingle_relationship(shingle, side, &1, bucket))
  end
  defp shingle_subshingle_relationship(shingle, side, subshingle, bucket) do
    predicate = case side do
      :beginning -> "ENDS_WITH"
      :end -> "BEGINS_WITH"
    end
    """
      MERGE (n:Shingle #{node_props(shingle, side)})
      #{bucket_property("n", bucket)}
      MERGE (m:Shingle #{node_props(subshingle)})
      #{bucket_property("m", bucket)}
      MERGE (n)-[r1:#{predicate}]->(m)
      #{bucket_property("r1", bucket)}
      WITH m
      MATCH (s:Shingle)-[:ENDS_WITH]->(m)<-[:BEGINS_WITH]-(t:Shingle)
      MERGE (s)-[r2:LEADS]->(t)
      #{bucket_property("r2", bucket)}
      MERGE (s)<-[r3:FOLLOWS]-(t)
      #{bucket_property("r3", bucket)}
    """
  end
  defp bucket_property(ref, bucket) do
    bucket_escaped = Poison.encode! bucket
    """
      ON CREATE SET #{ref}.bucket=[#{bucket_escaped}]
      ON MATCH SET #{ref}.bucket = FILTER(x in #{ref}.bucket WHERE NOT(x=#{bucket_escaped})) + #{bucket_escaped}
    """
  end
  defp node_props(shingle, side \\ nil) do
    text = Enum.join(shingle)
    text_escaped = Poison.encode! text
    num_tokens = length(shingle)
    side_part = if side do
      side_string = Atom.to_string side
      ", side: \"#{side_string}\""
    else
      ""
    end
    "{text: #{text_escaped}, numTokens: #{num_tokens}, length: #{String.length(text)}#{side_part}}"
  end
  
  def random_path(source_bucket, target_bucket) do
    # Might not be the most efficient way to query for a random node in large buckets,
    # but I'm not exactly expecting huge amounts of overlap between buckets
    # Could optimize this further by taking a sampling where clause, ie `WHERE rand() > 0.5`
    source_escaped = Poison.encode! source_bucket
    target_escaped = Poison.encode! target_bucket
    """
    MATCH p=(n:Shingle {side: "beginning"})-[:LEADS]->(m:Shingle {side: "end"})
    WHERE #{source_escaped} in n.bucket AND #{target_escaped} in m.bucket
    WITH p, rand() AS r
    ORDER BY r
    RETURN p
    LIMIT 1
    """
  end
end
