/**
 *    Copyright (C) 2013 10gen Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#include "mongo/db/ops/modifier_diff.h"
#include "mongo/base/error_codes.h"
#include "mongo/bson/mutable/document.h"
#include "mongo/db/ops/field_checker.h"
#include "mongo/db/ops/log_builder.h"
#include "mongo/db/ops/path_support.h"
#include "mongo/db/query/collation/collator_interface.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

namespace str = mongoutils::str;

struct ModifierDiff::PreparedState {
    PreparedState(mutablebson::Document& targetDoc)
        : doc(targetDoc), idxFound(0), elemFound(doc.end()), noOp(false) {}

    // Document that is going to be changed.
    mutablebson::Document& doc;

    // Index in _fieldRef for which an Element exist in the document.
    size_t idxFound;

    // Element corresponding to _fieldRef[0.._idxFound].
    mutablebson::Element elemFound;

	// This $diff is a no-op?
	bool noOp;
};

ModifierDiff::ModifierDiff()
    : _pathReplacementPosition(0) {}

ModifierDiff::~ModifierDiff() {}

Status ModifierDiff::init(const BSONElement& modExpr, const Options& opts, bool* positional) {
    _updatePath.parse(modExpr.fieldName());
    Status status = fieldchecker::isUpdatable(_updatePath);
    if (!status.isOK()) {
        return status;
    }

    // If a $-positional operator was used, get the index in which it occurred
    // and ensure only one occurrence.
    size_t foundCount;
    bool foundDollar =
        fieldchecker::isPositional(_updatePath, &_pathReplacementPosition, &foundCount);

    if (positional)
        *positional = foundDollar;

    if (foundDollar && foundCount > 1) {
        return Status(ErrorCodes::BadValue,
                      str::stream() << "Too many positional (i.e. '$') elements found in path '"
                                    << _updatePath.dottedField()
                                    << "'");
    }

    // Store value for later.
    _val = modExpr;
    return Status::OK();
}

Status ModifierDiff::prepare(mutablebson::Element root,
                                StringData matchedField,
                                ExecInfo* execInfo) {
    _preparedState.reset(new PreparedState(root.getDocument()));

    // If we have a $-positional field, it is time to bind it to an actual field part.
    if (_pathReplacementPosition) {
        if (matchedField.empty()) {
            return Status(ErrorCodes::BadValue,
                          str::stream() << "The positional operator did not find the match "
                                           "needed from the query. Unexpanded update: "
                                        << _updatePath.dottedField());
        }
        _updatePath.setPart(_pathReplacementPosition, matchedField);
    }

    // Locate the field name in 'root'. Note that we may not have all the parts in the path
    // in the doc -- which is fine. Our goal now is merely to reason about whether this mod
    // apply is a noOp or whether is can be in place. The remaining path, if missing, will
    // be created during the apply.
    Status status = pathsupport::findLongestPrefix(
        _updatePath, root, &_preparedState->idxFound, &_preparedState->elemFound);

    // FindLongestPrefix may say the path does not exist at all, which is fine here, or
    // that the path was not viable or otherwise wrong, in which case, the mod cannot
    // proceed.
    if (status.code() == ErrorCodes::NonExistentPath) {
        _preparedState->elemFound = root.getDocument().end();
    } else if (!status.isOK()) {
        return status;
    }

    // We register interest in the field name. The driver needs this info to sort out if
    // there is any conflict among mods.
    execInfo->fieldRef[0] = &_updatePath;

    const bool destExists = (_preparedState->elemFound.ok() &&
                             _preparedState->idxFound == (_updatePath.numParts() - 1));
    if (!destExists) {
        execInfo->noOp = _preparedState->noOp = true;
    } else {
        const int compareVal =
            _preparedState->elemFound.compareWithBSONElement(_val, nullptr, false);
		execInfo->noOp = _preparedState->noOp = (compareVal != 0);
    }

    return Status::OK();
}

Status ModifierDiff::apply() const {
	dassert(!_preparedState->noOp);

	// Our semantics says that, if we're unseting an element of an array, we swap that
	// value to null. The rationale is that we don't want other array elements to change
	// indices. (That could be achieved with $pull-ing element from it.)
	if (_preparedState->elemFound.parent().ok() &&
		_preparedState->elemFound.parent().getType() == Array) {
		return _preparedState->elemFound.setValueNull();
	}
	else {
		return _preparedState->elemFound.remove();
	}
}

Status ModifierDiff::log(LogBuilder* logBuilder) const {
    return logBuilder->addToUnsets(_updatePath.dottedField());
}

}  // namespace mongo
