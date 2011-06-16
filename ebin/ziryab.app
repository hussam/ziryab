{application, ziryab,
   [
      {description, "Scalabe strongly consistent KVS"},
      {vsn, "0.1"},
      {modules, [
         core,
         elastic_replica,
         replica,
         repobj,
         utils,
         kv_core,
         kvs,
         kvstracker
         ]},
      {registered, [kvstracker]},
      {applications, [kernel, stdlib]}
   ]
}.
