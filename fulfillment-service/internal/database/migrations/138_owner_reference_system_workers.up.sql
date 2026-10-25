--
-- Copyright (c) 2026 Red Hat Inc.
--
-- Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
-- the License. You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
-- an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
-- specific language governing permissions and limitations under the License.
--

-- CaaS worker BareMetalInstances live in tenant system (CaaS networking design, OSAC-1436) and reference the Cluster
-- they serve, which lives in the tenant of its owner. The owner reference check of migration 125 therefore accepts a
-- reference from a row of tenant system to a resource of any tenant. Every other check is unchanged.
create or replace function check_owner_reference() returns trigger as $$
declare
  owner_reference jsonb;
  set_once_path text;
  target_table text;
  target_id text;
  found_id text;
  found_tenant text;
  found_project ltree;
  found_deletion timestamptz;
begin
  if TG_OP = 'UPDATE' then
    foreach set_once_path in array coalesce(owner_reference_set_once_paths(TG_TABLE_NAME), '{}'::text[]) loop
      if coalesce(new.data #> string_to_array(set_once_path, '.'), 'null'::jsonb) is distinct from
         coalesce(old.data #> string_to_array(set_once_path, '.'), 'null'::jsonb) then
        raise exception using
          errcode = 'Z0005',
          message = format(
            'cannot change %s of %s ''%s'': it is set once at creation',
            set_once_path, TG_TABLE_NAME, new.id
          );
      end if;
    end loop;
    return new;
  end if;

  owner_reference := new.data #> string_to_array(owner_reference_path(TG_TABLE_NAME), '.');
  if owner_reference is null then
    return new;
  end if;

  target_table := owner_reference_target_table(TG_TABLE_NAME);
  if target_table is null then
    return new;
  end if;

  target_id := owner_reference->>'id';
  if coalesce(target_id, '') = '' then
    raise exception using
      errcode = 'Z0005',
      message = format(
        'the owner reference of %s ''%s'' must set the target id',
        TG_TABLE_NAME, new.id
      );
  end if;

  execute format('select id, tenant, project, deletion_timestamp from %I where id = $1 for share', target_table)
    into found_id, found_tenant, found_project, found_deletion
    using target_id;

  if found_id is null then
    raise exception using
      errcode = 'Z0005',
      message = format(
        'the owner reference of %s ''%s'' targets a missing %s ''%s''',
        TG_TABLE_NAME, new.id, target_table, target_id
      );
  end if;
  if found_deletion != 'epoch' then
    raise exception using
      errcode = 'Z0005',
      message = format(
        'the owner reference of %s ''%s'' targets a deleting %s ''%s''',
        TG_TABLE_NAME, new.id, target_table, target_id
      );
  end if;
  if new.tenant != 'system' and (found_tenant != new.tenant or found_project != new.project) then
    raise exception using
      errcode = 'Z0005',
      message = format(
        'the owner reference of %s ''%s'' must target a %s in the same tenant and project',
        TG_TABLE_NAME, new.id, target_table
      );
  end if;

  return new;
end;
$$ language plpgsql;
