\* Hi-lock: (("\\\\\\*.+$" (0 (quote org-level-3) t)))
\* Hi-lock: (("^.*\\(?:==\\).*$" (0 (quote org-level-1) t)))
------------- MODULE pan3 --------------------
EXTENDS Integers, Sequences
CONSTANT N, K, M
ASSUME M \in Nat
ASSUME N \in Nat
ASSUME K < 20
Master == 0..M
Procs == M+1..N
Vars == 1..K

SetMin(S) == CHOOSE i \in S : \A j \in S : i <= j

(* pan3 builds on pan2 by adding lock tokens
 --algorithm pan3
 { variable reqq = [y \in Vars |-> <<>>], \* s-> m  :maintained at the master
            recall=[j \in Procs |-> {}], \* m->s  :maintained at the servers
            flag=[z \in Vars |-> 0]; \* m <->s  :bothways, initially all locks at master 

   \* Server processes
   fair+ process (p \in Procs)
   variables w=0, q=0, reqset={}, lockset={}; 
   { ncs: while (TRUE)
   { either { \* release cached lock
     release: if (recall[self] # {}/\ \E i \in recall[self]: i \notin lockset) {
                   q:= CHOOSE i \in recall[self]: i \notin lockset;
		   flag[q]:=0; \* send it back to the master
                   recall[self] := recall[self] \ {q};
               }
        }
     or { 
     thinking: while (reqset={})
                  with (S \in SUBSET Vars) {reqset:=reqset \union S;};      
     hungry: while (reqset # {})
                 { w:= SetMin(reqset); \* request locks in increasing order
                   if (flag[w] # self)  \* don't reqq if you already cached the lock
                      {reqq[w] := Append(reqq[w], self);};
                   reqset := reqset \ {w};
                   lockset := lockset \union {w};
                 };
     waiting: await(\A x \in lockset: flag[x]=self); 
     cs:      skip; \* the critical section  
     exit:    lockset:= {}; \* Release all locks; lazy unlock
     }\* end or
   }\* end while
   }\* end process p 


   \* this is just a thread in lock broker; there can be M such master threads
   fair+ process (m \in Master)
   variables nxti=0, v=0;
   { master: while (TRUE)  
	       { either
	           {
             \* Serve locks going smaller to larger.
             \* can relax if I add authoritative lock in server, but this is simple
	    serve:  if (v=0 /\ \E x \in Vars: Len(reqq[x])>0)
                  { v := CHOOSE k \in Vars: Len (reqq[k])>0 /\ (\A z \in Vars: Len (reqq[z])>0 => k<=z); \* Serve locks going smaller to larger !!!
                     nxti:= Head(reqq[v]);
                     reqq[v]:=Tail(reqq[v]);};
                }
             or{   
        grant:  if (v#0 /\ flag[v]=0) 
                   {flag[v]:=nxti; 
                    v:=0;};
                 else if (v#0 /\ flag[v]#0)
                   {recall[flag[v]]:= recall[flag[v]] \union {v};};
                }
           }\* end while
   }\* end process p 
 }\* end algorithm
*)
\* BEGIN TRANSLATION
VARIABLES reqq, recall, flag, pc, w, q, reqset, lockset, nxti, v

vars == << reqq, recall, flag, pc, w, q, reqset, lockset, nxti, v >>

ProcSet == (Procs) \cup (Master)

Init == (* Global variables *)
        /\ reqq = [y \in Vars |-> <<>>]
        /\ recall = [j \in Procs |-> {}]
        /\ flag = [z \in Vars |-> 0]
        (* Process p *)
        /\ w = [self \in Procs |-> 0]
        /\ q = [self \in Procs |-> 0]
        /\ reqset = [self \in Procs |-> {}]
        /\ lockset = [self \in Procs |-> {}]
        (* Process m *)
        /\ nxti = [self \in Master |-> 0]
        /\ v = [self \in Master |-> 0]
        /\ pc = [self \in ProcSet |-> CASE self \in Procs -> "ncs"
                                        [] self \in Master -> "master"]

ncs(self) == /\ pc[self] = "ncs"
             /\ \/ /\ pc' = [pc EXCEPT ![self] = "release"]
                \/ /\ pc' = [pc EXCEPT ![self] = "thinking"]
             /\ UNCHANGED << reqq, recall, flag, w, q, reqset, lockset, nxti, 
                             v >>

release(self) == /\ pc[self] = "release"
                 /\ IF recall[self] # {}/\ \E i \in recall[self]: i \notin lockset[self]
                       THEN /\ q' = [q EXCEPT ![self] = CHOOSE i \in recall[self]: i \notin lockset[self]]
                            /\ flag' = [flag EXCEPT ![q'[self]] = 0]
                            /\ recall' = [recall EXCEPT ![self] = recall[self] \ {q'[self]}]
                       ELSE /\ TRUE
                            /\ UNCHANGED << recall, flag, q >>
                 /\ pc' = [pc EXCEPT ![self] = "ncs"]
                 /\ UNCHANGED << reqq, w, reqset, lockset, nxti, v >>

thinking(self) == /\ pc[self] = "thinking"
                  /\ IF reqset[self]={}
                        THEN /\ \E S \in SUBSET Vars:
                                  reqset' = [reqset EXCEPT ![self] = reqset[self] \union S]
                             /\ pc' = [pc EXCEPT ![self] = "thinking"]
                        ELSE /\ pc' = [pc EXCEPT ![self] = "hungry"]
                             /\ UNCHANGED reqset
                  /\ UNCHANGED << reqq, recall, flag, w, q, lockset, nxti, v >>

hungry(self) == /\ pc[self] = "hungry"
                /\ IF reqset[self] # {}
                      THEN /\ w' = [w EXCEPT ![self] = SetMin(reqset[self])]
                           /\ IF flag[w'[self]] # self
                                 THEN /\ reqq' = [reqq EXCEPT ![w'[self]] = Append(reqq[w'[self]], self)]
                                 ELSE /\ TRUE
                                      /\ reqq' = reqq
                           /\ reqset' = [reqset EXCEPT ![self] = reqset[self] \ {w'[self]}]
                           /\ lockset' = [lockset EXCEPT ![self] = lockset[self] \union {w'[self]}]
                           /\ pc' = [pc EXCEPT ![self] = "hungry"]
                      ELSE /\ pc' = [pc EXCEPT ![self] = "waiting"]
                           /\ UNCHANGED << reqq, w, reqset, lockset >>
                /\ UNCHANGED << recall, flag, q, nxti, v >>

waiting(self) == /\ pc[self] = "waiting"
                 /\ (\A x \in lockset[self]: flag[x]=self)
                 /\ pc' = [pc EXCEPT ![self] = "cs"]
                 /\ UNCHANGED << reqq, recall, flag, w, q, reqset, lockset, 
                                 nxti, v >>

cs(self) == /\ pc[self] = "cs"
            /\ TRUE
            /\ pc' = [pc EXCEPT ![self] = "exit"]
            /\ UNCHANGED << reqq, recall, flag, w, q, reqset, lockset, nxti, v >>

exit(self) == /\ pc[self] = "exit"
              /\ lockset' = [lockset EXCEPT ![self] = {}]
              /\ pc' = [pc EXCEPT ![self] = "ncs"]
              /\ UNCHANGED << reqq, recall, flag, w, q, reqset, nxti, v >>

p(self) == ncs(self) \/ release(self) \/ thinking(self) \/ hungry(self)
              \/ waiting(self) \/ cs(self) \/ exit(self)

master(self) == /\ pc[self] = "master"
                /\ \/ /\ pc' = [pc EXCEPT ![self] = "serve"]
                   \/ /\ pc' = [pc EXCEPT ![self] = "grant"]
                /\ UNCHANGED << reqq, recall, flag, w, q, reqset, lockset, 
                                nxti, v >>

serve(self) == /\ pc[self] = "serve"
               /\ IF v[self]=0 /\ \E x \in Vars: Len(reqq[x])>0
                     THEN /\ v' = [v EXCEPT ![self] = CHOOSE k \in Vars: Len (reqq[k])>0 /\ (\A z \in Vars: Len (reqq[z])>0 => k<=z)]
                          /\ nxti' = [nxti EXCEPT ![self] = Head(reqq[v'[self]])]
                          /\ reqq' = [reqq EXCEPT ![v'[self]] = Tail(reqq[v'[self]])]
                     ELSE /\ TRUE
                          /\ UNCHANGED << reqq, nxti, v >>
               /\ pc' = [pc EXCEPT ![self] = "master"]
               /\ UNCHANGED << recall, flag, w, q, reqset, lockset >>

grant(self) == /\ pc[self] = "grant"
               /\ IF v[self]#0 /\ flag[v[self]]=0
                     THEN /\ flag' = [flag EXCEPT ![v[self]] = nxti[self]]
                          /\ v' = [v EXCEPT ![self] = 0]
                          /\ UNCHANGED recall
                     ELSE /\ IF v[self]#0 /\ flag[v[self]]#0
                                THEN /\ recall' = [recall EXCEPT ![flag[v[self]]] = recall[flag[v[self]]] \union {v[self]}]
                                ELSE /\ TRUE
                                     /\ UNCHANGED recall
                          /\ UNCHANGED << flag, v >>
               /\ pc' = [pc EXCEPT ![self] = "master"]
               /\ UNCHANGED << reqq, w, q, reqset, lockset, nxti >>

m(self) == master(self) \/ serve(self) \/ grant(self)

Next == (\E self \in Procs: p(self))
           \/ (\E self \in Master: m(self))

Spec == /\ Init /\ [][Next]_vars
        /\ \A self \in Procs : SF_vars(p(self))
        /\ \A self \in Master : SF_vars(m(self))

\* END TRANSLATION

------------------------------------------------------
NaiveME == \A i,j \in Procs : (i # j) => ~ (pc[i] = "cs" /\ pc[j] = "cs") \* This shall be violated!!!
MutualExclusion == \A i,j \in Procs : (i # j) => ~ (pc[i] = "cs" /\ pc[j] = "cs" /\ (\E z \in Vars: flag[z]=i /\ flag[z]=j))
StarvationFree == \A i \in Procs: pc[i]="waiting" ~> pc[i] = "cs"
====================================================== 


