\* Hi-lock: (("\\\\\\*.+$" (0 (quote org-level-3) t)))
\* Hi-lock: (("^.*\\(?:==\\).*$" (0 (quote org-level-1) t)))
------------- MODULE pan --------------------
EXTENDS Integers, Sequences
CONSTANT N, K, M
ASSUME M \in Nat /\ N \in Nat /\ K < 20
Master == 0..M
Procs == M+1..N
Vars == 1..K

SetMin(S) == CHOOSE i \in S : \A j \in S : i <= j

(* a revision of pan3
 --algorithm pan
 { variable reqq = [k \in  Vars |-> <<>>], \* s-> m  :maintained at the master
            recall=[i \in Procs |-> [k \in Vars |-> <<>>]], \* m->s  :maintained at the servers
            flag=[k \in Vars |-> 0]; \* m <->s  :bothways, initially all locks at master 

   fair+ process (p \in Procs)
   variables w=0, y=0, reqset={}, lockset={};
   { ncs: while (TRUE)
   { either { \* release cached lock
     release: if (\E i \in Vars: Len(recall[self][i])>0 /\ i \notin lockset) {
                 y := CHOOSE k \in Vars: Len(recall[self][k])>0 /\ k \notin lockset;
                 flag[y]:= Head(recall[self][y]); \* send variable to server at head of recallq
                 recall[Head(recall[y])][y] := Tail (recall[y]); \* forward recallq to said server
     reldone:    recall[self][y] := <<>>; \* reset recallq
               }
        }
     or { 
     thinking: while (reqset={})
                  with (S \in SUBSET Vars) {reqset:=reqset \union S;};      
     hungry: while (reqset # {}) \* atomic request!
                 { w:= SetMin(reqset); \* request locks in increasing order
                   if (flag[w] # self)  \* don't reqq if you already cached the lock
                      {reqq[w] := Append(reqq[w], self);};
                   reqset := reqset \ {w};
                   lockset := lockset \union {w};
                 };
     waiting: await(\A x \in lockset: flag[x]=self); 
     cs:      skip; \* the critical section  
     exit:    lockset := {}; \* lazy unlock optimization
     }\* end or
   }\* end while
   }\* end process p 


   \* this is just a thread in lock broker; it is a nonblocking thread as well. 
   fair+ process (m \in Master)
   variables nxti=0, v=0;
   { master: while (TRUE){
	         await (\E x \in Vars: Len(reqq[x])>0);
             \* Serve locks going smaller to larger, it is nonblocking!
             \* can relax if I add authoritative lock in server, but this is simple & elegant
             v := CHOOSE k \in Vars: Len (reqq[k])>0 /\ (\A z \in Vars: Len (reqq[z])>0 => k<=z); 
             nxti:= Head(reqq[v]);
             reqq[v]:=Tail(reqq[v]);
             if  (flag[v]=0) 
                  flag[v]:=nxti; 
             else recall[nxti][flag[v]]:= Append (recall[nxti][flag[v]], nxti);
           }\* end while
   }\* end process p 
 }\* end algorithm
*)

\* BEGIN TRANSLATION
VARIABLES reqq, recall, flag, pc, w, y, reqset, lockset, nxti, v

vars == << reqq, recall, flag, pc, w, y, reqset, lockset, nxti, v >>

ProcSet == (Procs) \cup (Master)

Init == (* Global variables *)
        /\ reqq = [k \in  Vars |-> <<>>]
        /\ recall = [i \in Procs |-> [k \in Vars |-> <<>>]]
        /\ flag = [k \in Vars |-> 0]
        (* Process p *)
        /\ w = [self \in Procs |-> 0]
        /\ y = [self \in Procs |-> 0]
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
             /\ UNCHANGED << reqq, recall, flag, w, y, reqset, lockset, nxti, 
                             v >>

release(self) == /\ pc[self] = "release"
                 /\ IF \E i \in Vars: Len(recall[self][i])>0 /\ i \notin lockset[self]
                       THEN /\ y' = [y EXCEPT ![self] = CHOOSE k \in Vars: Len(recall[self][k])>0 /\ k \notin lockset[self]]
                            /\ flag' = [flag EXCEPT ![y'[self]] = Head(recall[self][y'[self]])]
                            /\ recall' = [recall EXCEPT ![Head(recall[y'[self]])][y'[self]] = Tail (recall[y'[self]])]
                            /\ pc' = [pc EXCEPT ![self] = "reldone"]
                       ELSE /\ pc' = [pc EXCEPT ![self] = "ncs"]
                            /\ UNCHANGED << recall, flag, y >>
                 /\ UNCHANGED << reqq, w, reqset, lockset, nxti, v >>

reldone(self) == /\ pc[self] = "reldone"
                 /\ recall' = [recall EXCEPT ![self][y[self]] = <<>>]
                 /\ pc' = [pc EXCEPT ![self] = "ncs"]
                 /\ UNCHANGED << reqq, flag, w, y, reqset, lockset, nxti, v >>

thinking(self) == /\ pc[self] = "thinking"
                  /\ IF reqset[self]={}
                        THEN /\ \E S \in SUBSET Vars:
                                  reqset' = [reqset EXCEPT ![self] = reqset[self] \union S]
                             /\ pc' = [pc EXCEPT ![self] = "thinking"]
                        ELSE /\ pc' = [pc EXCEPT ![self] = "hungry"]
                             /\ UNCHANGED reqset
                  /\ UNCHANGED << reqq, recall, flag, w, y, lockset, nxti, v >>

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
                /\ UNCHANGED << recall, flag, y, nxti, v >>

waiting(self) == /\ pc[self] = "waiting"
                 /\ (\A x \in lockset[self]: flag[x]=self)
                 /\ pc' = [pc EXCEPT ![self] = "cs"]
                 /\ UNCHANGED << reqq, recall, flag, w, y, reqset, lockset, 
                                 nxti, v >>

cs(self) == /\ pc[self] = "cs"
            /\ TRUE
            /\ pc' = [pc EXCEPT ![self] = "exit"]
            /\ UNCHANGED << reqq, recall, flag, w, y, reqset, lockset, nxti, v >>

exit(self) == /\ pc[self] = "exit"
              /\ lockset' = [lockset EXCEPT ![self] = {}]
              /\ pc' = [pc EXCEPT ![self] = "ncs"]
              /\ UNCHANGED << reqq, recall, flag, w, y, reqset, nxti, v >>

p(self) == ncs(self) \/ release(self) \/ reldone(self) \/ thinking(self)
              \/ hungry(self) \/ waiting(self) \/ cs(self) \/ exit(self)

master(self) == /\ pc[self] = "master"
                /\ (\E x \in Vars: Len(reqq[x])>0)
                /\ v' = [v EXCEPT ![self] = CHOOSE k \in Vars: Len (reqq[k])>0 /\ (\A z \in Vars: Len (reqq[z])>0 => k<=z)]
                /\ nxti' = [nxti EXCEPT ![self] = Head(reqq[v'[self]])]
                /\ reqq' = [reqq EXCEPT ![v'[self]] = Tail(reqq[v'[self]])]
                /\ IF flag[v'[self]]=0
                      THEN /\ flag' = [flag EXCEPT ![v'[self]] = nxti'[self]]
                           /\ UNCHANGED recall
                      ELSE /\ recall' = [recall EXCEPT ![nxti'[self]][flag[v'[self]]] = Append (recall[nxti'[self]][flag[v'[self]]], nxti'[self])]
                           /\ flag' = flag
                /\ pc' = [pc EXCEPT ![self] = "master"]
                /\ UNCHANGED << w, y, reqset, lockset >>

m(self) == master(self)

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

