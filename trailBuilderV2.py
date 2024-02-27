import os
import sys
import re
import json
import copy


class Node:

    def __init__(self, data):
        self.Parent = None
        self.key = None
        self.data = data
        self.Children = []

    def get_key(self, new_id_on_modify = False):

        event_type = self.data['type']
        parentKey = None
        CATReporterIMID = self.data.get('CATReporterIMID')
        symbol = self.data.get('symbol', False)
        if not symbol:
            symbol = self.data.get('optionID') 


        if self.data.get('parentOrderID'):
            parentKey = self.data.get('parentOrderID')
            parentKeyDate = self.data.get('parentOrderKeyDate')
        elif self.data.get('priorOrderID'):
            parentKey = self.data.get('priorOrderID')
            parentKeyDate = self.data.get('priorOrderKeyDate')

        if parentKey: 
            self.Parent = CATReporterIMID+'_'+symbol+'_'+parentKey+'_'+parentKeyDate

        if self.data.get('tradeID'):
            key = self.data.get('tradeID')
            keyDate = self.data.get('tradeKeyDate')
            if 'sellDetails' in self.data:
                parentKey = self.data.get('sellDetails')[0].get('orderID')
                parentKeyDate = self.data.get('sellDetails')[0].get('orderKeyDate')
                self.Parent = CATReporterIMID+'_'+symbol+'_'+parentKey+'_'+parentKeyDate
            elif 'buyDetails' in self.data:
                parentKey = self.data.get('buyDetails')[0].get('orderID')
                parentKeyDate = self.data.get('buyDetails')[0].get('orderKeyDate')
                self.Parent = CATReporterIMID+'_'+symbol+'_'+parentKey+'_'+parentKeyDate


        elif self.data.get('fulfillmentID'):
            key = self.data.get('fulfillmentID')
            keyDate = self.data.get('fillkeyDate')
            if 'clientDetails' in self.data:
                parentKey = self.data.get('clientDetails')[0].get('orderID')
                parentKeyDate = self.data.get('clientDetails')[0].get('orderKeyDate')
                self.Parent = CATReporterIMID+'_'+symbol+'_'+parentKey+'_'+parentKeyDate
            elif 'firmDetails' in self.data:
                parentKey = self.data.get('firmDetails')[0].get('orderID')
                parentKeyDate = self.data.get('firmDetails')[0].get('orderKeyDate')
                self.Parent = CATReporterIMID+'_'+symbol+'_'+parentKey+'_'+parentKeyDate


        # elif self.data.get('routedOrderID'):
        #     key = self.data.get('routedOrderID')
        #     keyDate = self.data.get('fillkeyDate')
        #     if priorRouteID:
        #         self.Parent = priorRouteID
        #     else:
        #         self.Parent = orderID
        #     return routeID

        elif self.data.get('orderID'):
            key = self.data.get('orderID')
            keyDate = self.data.get('orderKeyDate')

            cancel_replace_bool = event_type[-1] in {'C', 'M', 'S'} or event_type[:-2] in {'CR', 'MR'}
          
            if cancel_replace_bool and not new_id_on_modify:
                self.Parent = CATReporterIMID+symbol+key+keyDate
        
        self.key = CATReporterIMID+symbol+key+keyDate

    def add_key(self, all_nodes):
        key_collisions = all_nodes.get(self.key)

        if key_collisions:

            self_timestamp = self.data.get('eventTimestamp')
            for i in range(len(key_collisions)):
                curr_timestamp = key_collisions[i].data.get('eventTimestamp')

                # The purpose of this code is to add the events with the same keys in time sequence order
                if self_timestamp < curr_timestamp:

                    if i == 0:
                        all_nodes[self.key] = [self]+all_nodes[self.key]
                    elif i == len(key_collisions)-1:
                        all_nodes[self.key] = all_nodes[self.key]+[self]
                    else:
                        all_nodes[self.key] = all_nodes[self.key][:i]+[self]+all_nodes[self.key][i:]
        else:
            all_nodes[self.key] = [self]

        return all_nodes

    def display(self, lvl, **kwargs):
        filters = {k: v for k, v in kwargs.items() if v}
        # if self.data.get('orderID') == '2310130000000003':
        #     breakpoint()
        if lvl == 0:
            for filter_, val in filters.items():
                if self.data.get(filter_) != val:
                    return
        indent = '\t\t'*lvl
        print(f'{indent}{self.data["type"]} SeqNum: {lvl}')
        for k,v in kwargs.items():
            print(f'{indent}{k}, {self.data.get(k)}')
        print('')
        for child_node in self.Children:
            child_node.display(lvl + 1, **kwargs)

    def traverse_up(self, orderID):
        try:
            return self.Roots[str(orderID)]
        except Exception as e:
            print(self.Leaves[str(orderID)])
            return self.Leaves[str(orderID)]
    
    def sort(all_nodes):

        for node_list in all_nodes.values():

            for node in node_list:

                CATReporterIMID = node.data.get('CATReportedIMID')
                symbol = node.data.get('symbol', False)
                if not symbol:
                    symbol = node.data.get('optionID') 


                if not node.Parent:
                    if node.data.get('type') not in ('MENO', 'MEOA', 'MONO', 'MOOA', 'MLNO', 'MLOA'):
                        print(f"Node has no parent - {node.data}")

                elif node.Parent == node.key and node.data['type'] not in {'MEOR', 'MOOR', 'MEORS', 'MOORS','MEMR', 'MOMR', 'MEMRS', 'MOMRS','MECR', 'MOCR', 'MECRS', 'MOCRS'}:
                    if all_nodes.get(node.Parent):
                        parent = all_nodes[node.Parent]
                        parent.Children.append(node)
                    else:
                        print(f'Could not find parent for node {node.data["type"]} looking for parent {node.Parent} \n {node.data}')
                else:
                    if all_nodes.get(node.Parent):
                        parent_list = all_nodes.get(node.Parent)
                        
                        node_ts = float(node.data.get('eventTimestamp')[-8:])
                        smallest_time_diff = 99999999
                        closest_parent = None
                        for parent in parent_list:
                            parent_ts = float(parent.data.get('eventTimestamp')[-8:])
                            time_diff = node_ts - parent_ts
                            if time_diff < smallest_time_diff:
                                closest_parent = parent
                        closest_parent.Children.append(node)
                    else:
                        print(f'Could not find parent for node {node.data["type"]} looking for parent {node.Parent} \n {node.data}')

    


def main():


    all_nodes = {}

    cwd = 'Input/'
    file_re = re.compile('1234_ABCD_20240101_TEST_OrderEvents_000001.json')

    for file in os.listdir(cwd):
        if not file_re.search(file): continue
        inf = open(cwd + file, 'r', encoding="UTF-8")

        for line in inf:
            record = json.loads(line)
            new_node = Node(record)

            new_node.get_key()
            all_nodes = new_node.add_key(all_nodes)

        inf.close()

    Node.sort(all_nodes)
    breakpoint()

    return all_nodes
    


    # for key, node in Node.Roots.items():
    #     node.display(0, orderID=None, routedOrderID=None, eventTimestamp=None, accountHolderType=None,
    #                  deptType=None, symbol=None, quantity=None, side=None, routeRejectedFlag=None)



    # [x.event_type for x  in Event.heads]
    # temp = [x for x in Event.heads if x.event_type == 'MEIR']

    # allocation_dict = readAllocations()
    # breakpoint()
    # temp = [elem for elem in heads if elem.FDID == 'JIL' and elem.side == 'SL' and elem.symbol == 'F']
    # total = sum(int(elem.quantity) for elem in temp)

if __name__ == "__main__":

    main()

