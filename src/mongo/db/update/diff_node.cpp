/**
 * Copyright (C) 2017 MongoDB Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects
 * for all of the code used other than as permitted herein. If you modify
 * file(s) with this exception, you may extend this exception to your
 * version of the file(s), but you are not obligated to do so. If you do not
 * wish to do so, delete this exception statement from your version. If you
 * delete this exception statement from all source files in the program,
 * then also delete it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/update/diff_node.h"

#include "mongo/db/update/storage_validation.h"

#include "mongo/db/query/collation/collator_interface.h"

namespace mongo {

Status DiffNode::init(BSONElement modExpr,
                         const boost::intrusive_ptr<ExpressionContext>& expCtx) {
    invariant(modExpr.ok());
    _val = modExpr;
    return Status::OK();
}

ModifierNode::ModifyResult DiffNode::updateExistingElement(
    mutablebson::Element* element, std::shared_ptr<FieldRef> elementPath) const {
    const auto compareVal = element->compareWithBSONElement(_val, nullptr, false);
    if (compareVal != 0) {
        return ModifyResult::kNoOp;
    } else {
		auto parent = element->parent();

		invariant(parent.ok());
		if (!parent.isType(BSONType::Array)) {
			invariantOK(element->remove());
		}
		else {
			// Special case: An $unset on an array element sets it to null instead of removing it from
			// the array.
			invariantOK(element->setValueNull());
		}

        return ModifyResult::kNormalUpdate;
    }
}

void DiffNode::validateUpdate(mutablebson::ConstElement updatedElement, mutablebson::ConstElement leftSibling, mutablebson::ConstElement rightSibling, std::uint32_t recursionLevel, ModifyResult modifyResult) const {
	invariant(modifyResult == ModifyResult::kNormalUpdate);

	// We only need to check the left and right sibling to see if the removed element was part of a
	// now invalid DBRef.
	if (leftSibling.ok()) {
		storage_validation::storageValid(leftSibling, false, 0);
	}

	if (rightSibling.ok()) {
		storage_validation::storageValid(rightSibling, false, 0);
	}
}

void DiffNode::logUpdate(LogBuilder * logBuilder, StringData pathTaken, mutablebson::Element element, ModifyResult modifyResult) const {
	invariant(logBuilder);
	invariant(modifyResult == ModifyResult::kNormalUpdate);
	uassertStatusOK(logBuilder->addToUnsets(pathTaken));
}

}  // namespace mongo
