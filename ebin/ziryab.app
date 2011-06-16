{application, ziryab,
   [
      {description, "Scalabe strongly consistent KVS"},
      {vsn, "0.1"},
      {modules, [
         ziryab,
         ziryab_core,
         ziryab_tracker
         ]},
      {registered, [ziryab_tracker]},
      {applications, [kernel, stdlib]}
   ]
}.
