{application, ziryab,
   [
      {description, "Scalabe strongly consistent KVS"},
      {vsn, "0.1"},
      {modules, [
         ziryab,
         ziryab_backend,
         ziryab_client
         ]},
      {registered, []},
      {applications, [kernel, stdlib]}
   ]
}.
