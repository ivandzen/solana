-----------------------------------------------------------------------------------------------------------------------
-- Returns pre-accounts data for given transaction on a given slot
CREATE OR REPLACE FUNCTION get_pre_accounts_one_slot(
    current_slot BIGINT,
    max_write_version BIGINT,
    transaction_accounts BYTEA[]
)

RETURNS TABLE (
    pubkey BYTEA,
    slot BIGINT,
    write_version BIGINT,
    signature BYTEA,
    data BYTEA
)

AS $get_pre_accounts_one_slot$

BEGIN
  RETURN QUERY 
    SELECT --DISTINCT ON (acc.pubkey)
      acc.pubkey,
      acc.slot,
      acc.write_version,
      acc.txn_signature,
      acc.data
    FROM account AS acc
    WHERE
      acc.slot = current_slot
      AND acc.write_version < max_write_version
      AND acc.pubkey IN (SELECT * FROM unnest(transaction_accounts));
   -- ORDER BY
   --   acc.pubkey, acc.write_version DESC;
END;
$get_pre_accounts_one_slot$ LANGUAGE plpgsql;



-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_pre_accounts_branch(
  start_slot BIGINT, 
  max_write_version BIGINT,
  in_txn_signature BYTEA
)
  
RETURNS TABLE (
    pubkey BYTEA,
    slot BIGINT,
    write_version BIGINT,
    signature BYTEA,
    data BYTEA
)

AS $get_pre_accounts_branch$
DECLARE
  branch_slots BIGINT[];
  transaction_accounts BYTEA[];
BEGIN
  SELECT array_agg(ta.pubkey)
  INTO transaction_accounts
  FROM transaction_account AS ta
  WHERE position(in_txn_signature IN ta.signature) > 0;
        
  WITH RECURSIVE parents AS (
    SELECT 
        first.slot,
        first.parent,
        first.status
    FROM slot AS first
    WHERE first.slot = start_slot
    UNION
        SELECT 
            next.slot,
            next.parent,
            next.status
        FROM slot AS next
        INNER JOIN parents p ON p.parent = next.slot
  ) 
  SELECT array_agg(prnts.slot) 
  INTO branch_slots
  FROM parents AS prnts;
   
  RETURN QUERY
  SELECT DISTINCT ON (slot_results.pubkey)
    slot_results.pubkey,
    slot_results.slot,
    slot_results.write_version,
    slot_results.signature,
    slot_results.data
  FROM 
    unnest(branch_slots) AS current_slot,
    get_pre_accounts_one_slot(
                current_slot, 
                max_write_version, 
                transaction_accounts
          ) AS slot_results
  ORDER BY 
    slot_results.pubkey, 
    slot_results.write_version DESC;
END;
$get_pre_accounts_branch$ LANGUAGE plpgsql;
