
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION find_slot_on_longest_branch(req_id VARCHAR)
RETURNS BIGINT
AS $find_slot_on_longest_branch$
DECLARE
  current_slot BIGINT := NULL;
  current_slot_status VARCHAR := NULL;
  num_in_txn_slots INT := 0;
BEGIN
  -- start from topmost slot
  SELECT s.slot 
  INTO current_slot 
  FROM slot AS s 
  ORDER BY s.slot DESC LIMIT 1;
  
  LOOP
    -- get status of current slot
    SELECT s.status 
    INTO current_slot_status 
    FROM slot AS s
    WHERE s.slot = current_slot;
    
    -- already on rooted slot - stop iteration
    IF current_slot_status = 'rooted' THEN
      RETURN NULL;
    END IF;
    
    -- does current slot contain transaction ?
    EXECUTE format('SELECT COUNT(*) FROM txn_slots_%I AS txns WHERE txns.slot = $1', req_id)
    INTO num_in_txn_slots
    USING current_slot;
    
    -- if yes - it means we found slot with txn 
    -- on the longest branch - return it
    IF num_in_txn_slots <> 0 THEN
      RETURN current_slot;
    END IF;
    
    -- If no - go further into the past - select parent slot
    SELECT s.parent
    INTO current_slot
    FROM slot AS s
    WHERE s.slot = current_slot;
  END LOOP;
END;
$find_slot_on_longest_branch$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------------------------------------------
-- Returns pre-accounts data for given transaction on a given slot
CREATE OR REPLACE FUNCTION get_pre_accounts_one_slot(
    current_slot BIGINT,
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

AS $get_pre_accounts_one_slot$

BEGIN
  RETURN QUERY 
    SELECT DISTINCT ON (acc.pubkey)
      acc.pubkey,
      acc.slot,
      acc.write_version,
      acc.txn_signature,
      acc.data
    FROM account AS acc
    WHERE
      acc.slot = current_slot
      AND acc.write_version < max_write_version
      AND acc.pubkey IN
        (SELECT ta.pubkey
        FROM transaction_account AS ta
        WHERE position(in_txn_signature IN ta.signature) > 0)
    ORDER BY
      acc.pubkey DESC, acc.write_version DESC;
END;
$get_pre_accounts_one_slot$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_pre_accounts_branch(
  req_id VARCHAR,
  start_slot BIGINT, 
  max_write_version BIGINT,
  in_txn_signature BYTEA
)
  
RETURNS BIGINT --first rooted slot

AS $get_pre_accounts_branch$
DECLARE
  current_slot BIGINT := start_slot;
  current_slot_status VARCHAR := NULL;
  num_in_txn_slots INT := 0;
BEGIN  
  LOOP
    -- get status of current slot
    SELECT s.status 
    INTO current_slot_status 
    FROM slot AS s
    WHERE s.slot = current_slot;
    
    -- already on rooted slot - stop iteration
    IF current_slot_status = 'rooted' THEN
      RETURN current_slot;
    END IF;

    EXECUTE format('INSERT INTO 
                      results_%I(
                        pubkey, 
                        slot, 
                        write_version, 
                        signature,
                        data)
                    SELECT get_pre_accounts_one_slot($1, $2, $3)
                    ON CONFLICT (pubkey)
                    DO NOTHING', req_id)
    USING current_slot, max_write_version, in_txn_signature;
    
    -- If no - go further into the past - select parent slot
    SELECT s.parent
    INTO current_slot
    FROM slot AS s
    WHERE s.slot = current_slot;
  END LOOP;
END;
$get_pre_accounts_branch$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_pre_accounts_root(
    max_slot BIGINT,
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

AS $get_pre_accounts_root$

BEGIN
  RETURN QUERY 
    SELECT DISTINCT ON (acc.pubkey)
      acc.pubkey,
      acc.slot,
      acc.write_version,
      acc.txn_signature,
      acc.data
    FROM account AS acc
    WHERE
      acc.slot <= start_slot
      AND acc.write_version < max_write_version
      AND acc.pubkey IN
        (SELECT ta.pubkey
        FROM transaction_account AS ta
        WHERE position(in_txn_signature IN ta.signature) > 0)
      ORDER BY
        acc.pubkey DESC, acc.write_version DESC
END;
$get_pre_accounts_root$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_pre_accounts(req_id VARCHAR, in_txn_signature BYTEA) 
RETURNS TABLE (
    pubkey BYTEA,
    slot BIGINT,
    write_version BIGINT,
    signature BYTEA,
    data BYTEA
)

AS $get_pre_accounts$

DECLARE
  current_slot BIGINT := NULL;
  max_write_version BIGINT := NULL;
   
BEGIN  
  -- create temporary tables
  EXECUTE format('CREATE TEMP TABLE txn_slots_%I ( 
                    slot BIGINT 
                  ) ON COMMIT DROP', req_id);

  EXECUTE format('CREATE TEMP TABLE results_%I (
                    pubkey BYTEA PRIMARY KEY,
                    slot BIGINT,
                    write_version BIGINT,
                    signature BYTEA,
                    data BYTEA
                  ) ON COMMIT DROP;', req_id);
                  
  -- Query minimum write version of account update
  SELECT 
    MIN(acc.write_version)
  INTO max_write_version
  FROM account AS acc
  WHERE position(in_txn_signature in acc.txn_signature) > 0;
 
  
  -- find all occurencies of transaction in slots
  EXECUTE format('INSERT INTO txn_slots_%I
                    SELECT txn.slot
                    FROM transaction AS txn
                    WHERE position($1 in txn.signature) > 0', req_id)
  USING in_txn_signature;

  -- try to find slot that was rooted with given transaction 
  EXECUTE format('SELECT ts.slot 
                    FROM txn_slots_%I AS ts
                    INNER JOIN slot AS s
                    ON ts.slot = s.slot
                    WHERE s.status = $1', req_id)
  USING 'rooted'
  INTO current_slot;
    
  
  IF current_slot = NULL THEN 
    -- No rooted slot found. It means transaction exist on some not finalized branch. 
    -- Try to find it on the longest one (search from topmost slot down to first rooted slot)
    SELECT find_slot_on_longest_branch(req_id) INTO current_slot;
    IF current_slot = NULL THEN
        -- Transaction not found on the longest branch - it exist somewhere on minor forks.
        -- Return empty list of accounts
        RETURN QUERY EXECUTE format('SELECT * FROM results_%I', req_id);
    END IF;
    
    -- Transaction found on the longest branch. Start searching recent states of accounts in this branch
    -- down to first rooted slot. This search algorithm iterates over parent slots and is slow.                    
    SELECT get_pre_accounts_branch(
      req_id, 
      current_slot, 
      max_write_version, 
      in_txn_signature) INTO current_slot;
  END IF;
  
  -- Transaction found on the rooted slot or restoring state on not finalized branch is finished.
  -- Start/Continue restoring state on rooted slots.
  
  EXECUTE format('INSERT INTO 
                      results_%I(
                        pubkey, 
                        slot, 
                        write_version, 
                        signature,
                        data)
                    SELECT get_pre_accounts_root($1, $2, $3)
                    ON CONFLICT (pubkey)
                    DO NOTHING', req_id)
    USING current_slot, max_write_version, in_txn_signature;
    
  RETURN QUERY EXECUTE format('SELECT * FROM results_%I', req_id);
    
END;
$get_pre_accounts$ LANGUAGE plpgsql;


SELECT encode(pubkey, 'hex'), slot, write_version, encode(signature, 'hex'), data FROM get_pre_accounts('test2322', '\x620ebe2670ced0c2ea61ddd62a63725c537f3f195c91703f6b6fdcd8c6a959df6f72bd26f29f3e766635f61af86a1b3ae64dc30e5e4df787784d4155fd036d0e'::bytea);
