#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

import heapq
import itertools

import sgqlc.operation

from . import github_schema

_schema = github_schema
_schema_root = _schema.github_schema


def get_query_pull_requests(owner, name, first, after, direction):
    kwargs = {"first": first, "order_by": {"field": "UPDATED_AT", "direction": direction}}
    if after:
        kwargs["after"] = after

    op = sgqlc.operation.Operation(_schema_root.query_type)
    repository = op.repository(owner=owner, name=name)
    repository.name()
    repository.owner.login()
    pull_requests = repository.pull_requests(**kwargs)
    pull_requests.nodes.__fields__(
        id="node_id",
        database_id="id",
        number=True,
        updated_at="updated_at",
        changed_files="changed_files",
        deletions=True,
        additions=True,
        merged=True,
        mergeable=True,
        can_be_rebased="can_be_rebased",
        maintainer_can_modify="maintainer_can_modify",
        merge_state_status="merge_state_status",
    )
    pull_requests.nodes.comments.__fields__(total_count=True)
    pull_requests.nodes.commits.__fields__(total_count=True)
    reviews = pull_requests.nodes.reviews(first=100, __alias__="review_comments")
    reviews.total_count()
    reviews.nodes.comments.__fields__(total_count=True)
    user = pull_requests.nodes.merged_by(__alias__="merged_by").__as__(_schema_root.User)
    user.__fields__(
        id="node_id",
        database_id="id",
        login=True,
        avatar_url="avatar_url",
        url="html_url",
        is_site_admin="site_admin",
    )
    pull_requests.page_info.__fields__(has_next_page=True, end_cursor=True)
    return str(op)


def get_query_reviews(owner, name, first, after, number=None):
    op = sgqlc.operation.Operation(_schema_root.query_type)
    repository = op.repository(owner=owner, name=name)
    repository.name()
    repository.owner.login()
    if number:
        pull_request = repository.pull_request(number=number)
    else:
        kwargs = {"first": first, "order_by": {"field": "UPDATED_AT", "direction": "ASC"}}
        if after:
            kwargs["after"] = after
        pull_requests = repository.pull_requests(**kwargs)
        pull_requests.page_info.__fields__(has_next_page=True, end_cursor=True)
        pull_request = pull_requests.nodes

    pull_request.__fields__(number=True, url=True)
    kwargs = {"first": first}
    if number and after:
        kwargs["after"] = after
    reviews = pull_request.reviews(**kwargs)
    reviews.page_info.__fields__(has_next_page=True, end_cursor=True)
    reviews.nodes.__fields__(
        id="node_id",
        database_id="id",
        body=True,
        state=True,
        url="html_url",
        author_association="author_association",
        submitted_at="submitted_at",
        created_at="created_at",
        updated_at="updated_at",
    )
    reviews.nodes.commit.oid()
    user = reviews.nodes.author(__alias__="user").__as__(_schema_root.User)
    user.__fields__(
        id="node_id",
        database_id="id",
        login=True,
        avatar_url="avatar_url",
        url="html_url",
        is_site_admin="site_admin",
    )
    return str(op)


def get_query_pull_request_review_comment_reactions(owner, name, first, after=None):
    op = sgqlc.operation.Operation(_schema_root.query_type)
    repository = op.repository(owner=owner, name=name)
    repository.name()
    repository.owner.login()

    kwargs = {"first": 100}
    if after:
        kwargs["after"] = after
    pull_requests = repository.pull_requests(**kwargs)
    pull_requests.page_info.__fields__(has_next_page=True, end_cursor=True)
    pull_requests.nodes.id(__alias__="node_id")

    reviews = pull_requests.nodes.reviews(first=3)
    reviews.page_info.__fields__(has_next_page=True, end_cursor=True)
    reviews.nodes.id(__alias__="node_id")
    reviews.nodes.database_id(__alias__="id")

    comments = reviews.nodes.comments(first=5)
    comments.page_info.__fields__(has_next_page=True, end_cursor=True)
    comments.nodes.id(__alias__="node_id")
    comments.nodes.database_id(__alias__="id")

    reactions = comments.nodes.reactions(first=10)
    reactions.page_info.__fields__(has_next_page=True, end_cursor=True)
    reactions.nodes.__fields__(id="node_id", database_id="id", content=True, created_at="created_at")
    user = reactions.nodes.user()
    user.__fields__(
        id="node_id",
        database_id="id",
        login=True,
        avatar_url="avatar_url",
        url="html_url",
        is_site_admin="site_admin",
    )
    # op.rate_limit.__fields__(limit=True, cost=True, remaining=True, reset_at=True)
    return str(op)


def get_query_review_comment_reactions(node_id, first, after):
    op = sgqlc.operation.Operation(_schema_root.query_type)
    pull_request = op.node(id=node_id).__as__(_schema_root.PullRequest)
    pull_request.id(__alias__="node_id")
    pull_request.repository.name()
    pull_request.repository.owner.login()

    kwargs = {"first": 3}
    if after:
        kwargs["after"] = after

    reviews = pull_request.reviews(**kwargs)
    reviews.page_info.__fields__(has_next_page=True, end_cursor=True)
    reviews.nodes.id(__alias__="node_id")
    reviews.nodes.database_id(__alias__="id")

    comments = reviews.nodes.comments(first=5)
    comments.page_info.__fields__(has_next_page=True, end_cursor=True)
    comments.nodes.id(__alias__="node_id")
    comments.nodes.database_id(__alias__="id")

    reactions = comments.nodes.reactions(first=10)
    reactions.page_info.__fields__(has_next_page=True, end_cursor=True)
    reactions.nodes.__fields__(id="node_id", database_id="id", content=True, created_at="created_at")
    user = reactions.nodes.user()
    user.__fields__(
        id="node_id",
        database_id="id",
        login=True,
        avatar_url="avatar_url",
        url="html_url",
        is_site_admin="site_admin",
    )
    return str(op)


def get_query_comment_reactions(node_id, first, after):
    op = sgqlc.operation.Operation(_schema_root.query_type)
    review = op.node(id=node_id).__as__(_schema_root.PullRequestReview)
    review.id(__alias__="node_id")
    review.repository.name()
    review.repository.owner.login()

    kwargs = {"first": 10}
    if after:
        kwargs["after"] = after

    comments = review.comments(**kwargs)
    comments.page_info.__fields__(has_next_page=True, end_cursor=True)
    comments.nodes.id(__alias__="node_id")
    comments.nodes.database_id(__alias__="id")

    reactions = comments.nodes.reactions(first=10)
    reactions.page_info.__fields__(has_next_page=True, end_cursor=True)
    reactions.nodes.__fields__(id="node_id", database_id="id", content=True, created_at="created_at")
    user = reactions.nodes.user()
    user.__fields__(
        id="node_id",
        database_id="id",
        login=True,
        avatar_url="avatar_url",
        url="html_url",
        is_site_admin="site_admin",
    )
    return str(op)


def get_query_reactions(node_id, first, after):
    op = sgqlc.operation.Operation(_schema_root.query_type)
    comment = op.node(id=node_id).__as__(_schema_root.PullRequestReviewComment)
    comment.id(__alias__="node_id")
    comment.database_id(__alias__="id")
    comment.repository.name()
    comment.repository.owner.login()

    kwargs = {"first": 100}
    if after:
        kwargs["after"] = after

    reactions = comment.reactions(**kwargs)
    reactions.page_info.__fields__(has_next_page=True, end_cursor=True)
    reactions.nodes.__fields__(id="node_id", database_id="id", content=True, created_at="created_at")
    user = reactions.nodes.user()
    user.__fields__(
        id="node_id",
        database_id="id",
        login=True,
        avatar_url="avatar_url",
        url="html_url",
        is_site_admin="site_admin",
    )
    return str(op)


class CursorStorage:
    Objects = ["PullRequest", "PullRequestReview", "PullRequestReviewComment", "Reaction"]

    def __init__(self):
        self.typename_to_prio = {o: prio for prio, o in enumerate(reversed(self.Objects))}
        self.count = itertools.count()
        self.storage = []

    def add_cursor(self, typename, cursor, parent_id=None):
        priority = self.typename_to_prio[typename]
        heapq.heappush(self.storage, (priority, next(self.count), (typename, cursor, parent_id)))

    def get_cursor(self):
        if self.storage:
            _, _, c = heapq.heappop(self.storage)
            res = {"typename": c[0], "cursor": c[1], "parent_id": c[2]}
            return res
