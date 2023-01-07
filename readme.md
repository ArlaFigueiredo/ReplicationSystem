# ReplicationSystem
Alunos: Arla Figueiredo, Juliana Ribeiro, Rafael Campos

Este projeto consiste em um trabalho para a Disciplina de Sistemas Distribuidos.

O objetivo é criar um sistema que tenha uma interface de cliente SQL e que efetue uma replicação ativa utilizandos os conceitos de comunicação em grupo.

Dessa forma cada base de dados representa um membro do grupo e neste grupo há um líder. O líder é o coordenador do grupo... ou seja ele que coordena em que ordem devem ser executados os comandos SQL enviados pelo cliente para manter a consistência e integredidade entre todas as replicas.

O líder deve monitorar os membros, excluindo membros defeituosos, assim como os membros tem que verificar se o líder ainda está ativo, e caso este fique fora por qualquer motivo os membros devem eleger um novo líder entre eles.

