import os
import sys
import re
import json
import copy


class TreeNode:

    Roots = {}
    Leaves = {}

    def __init__(self, data):
        self.Parent = None
        self.data = data
        self.Children = []

    def get_key(self):

        event_type = self.data['type']

        tradeID = self.data.get('tradeID')
        fillID = self.data.get('fulfillmentID')
        routeID = self.data.get('routedOrderID')
        orderID = self.data.get('orderID')
        parentID = self.data.get('parentOrderID')
        priorID = self.data.get('priorOrderID')
        priorRouteID = self.data.get('priorRoutedID')

        if tradeID:
            if 'sellDetails' in self.data:
                self.Parent = self.data.get('sellDetails')[0].get('orderID')
            elif 'buyDetails' in self.data:
                self.Parent = self.data.get('buyDetails')[0].get('orderID')
            return tradeID

        elif fillID:
            if 'clientDetails' in self.data:
                self.Parent = self.data.get('clientDetails')[0].get('orderID')
            elif 'firmDetails' in self.data:
                self.Parent = self.data.get('firmDetails')[0].get('orderID')
            return fillID

        elif routeID:

            if priorRouteID:
                self.Parent = priorRouteID
            else:
                self.Parent = orderID
            return routeID

        elif orderID:
            if parentID:
                self.Parent = parentID
            elif priorID:
                self.Parent = priorID
            elif event_type[-1] in {'C', 'M', 'S'} or event_type[:-2] in {'CR', 'MR'}:
                self.Parent = orderID
            return orderID

    def sort():
        for key, nodes in TreeNode.Leaves.items():
            for node in nodes:
                if not node.Parent: #and node.data['type'] not in {'MEOT', 'MEOF'}:
                    print("leaf has no parent")
                elif node.Parent == node.data.get('orderID') and node.data['type'] not in {'MEOR', 'MOOR', 'MEORS', 'MOORS',
                                                                                           'MEMR', 'MOMR', 'MEMRS', 'MOMRS',
                                                                                           'MECR', 'MOCR', 'MECRS', 'MOCRS'}:
                    if TreeNode.Roots.get(node.Parent):
                        parent = TreeNode.Roots[node.Parent]
                        parent.Children.append(node)
                    else:
                        print(f'Could not find parent for node {node.data["type"]} looking for parent {node.Parent}')
                else:
                    if TreeNode.Roots.get(node.Parent):
                        parent = TreeNode.Roots[node.Parent]
                        parent.Children.append(node)
                    elif TreeNode.Leaves.get(node.Parent):
                        parents = TreeNode.Leaves[node.Parent]
                        for parent in parents:
                            parent.Children.append(node)
                    else:
                        print(f'Could not find parent for node {node.data["type"]} looking for parent {node.Parent}')

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


def main():

    cwd = 'C:\\Users\\Jesus\\Documents\\CANTOR\\Input\\'
    primaryEvents = {'MENO', 'MONO', 'MEOA', 'MOOA', 'MLNO', 'MLOA'}

    fidessa_re = re.compile('All.json$')
    for file in os.listdir(cwd):
        if not fidessa_re.search(file): continue
        print(file)
        inf = open(cwd + file, 'r', encoding="UTF-8")
        for line in inf:
            record = json.loads(line)
            event_type = record['type']
            new_event = TreeNode(record)
            key = new_event.get_key()
            if event_type in primaryEvents:
                TreeNode.Roots[key] = new_event
            else:
                if key not in TreeNode.Leaves:
                    TreeNode.Leaves[key] = [new_event]
                else:
                    TreeNode.Leaves[key].append(new_event)

        inf.close()
    TreeNode.sort()
    for key, node in TreeNode.Roots.items():
        node.display(0, orderID=None, routedOrderID=None, eventTimestamp=None, accountHolderType=None,
                     deptType=None, symbol=None, quantity=None, side=None, routeRejectedFlag=None)



    # [x.event_type for x  in Event.heads]
    # temp = [x for x in Event.heads if x.event_type == 'MEIR']

    # allocation_dict = readAllocations()
    # breakpoint()
    # temp = [elem for elem in heads if elem.FDID == 'JIL' and elem.side == 'SL' and elem.symbol == 'F']
    # total = sum(int(elem.quantity) for elem in temp)




if __name__ == "__main__":
    main()
